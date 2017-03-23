/*
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	//	"encoding/json"
	"encoding/csv"
	"encoding/gob"
	"encoding/xml"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/simpledb"
	"github.com/hashicorp/go-memdb"
	gzip "github.com/klauspost/compress/gzip"
	"github.com/twinj/uuid"
	//	"compress/gzip"
	"reflect"
	"sync"
	"time"
)

type arguments struct {
	items map[string]interface{}
}

func (a arguments) check(target string, args arguments) bool {
	fmt.Println(args)
	// i := sort.SearchStrings(args.items, target)
	return false
}

type S3Document struct {
	S3Document               []byte
	S3VersionID              string
	S3DocumentID             string
	S3DocumentMetadata       string
	S3DocumentCompressedSize int64
	S3DocumentSize           int64
	S3DocumentLatest         bool
}

type PersistentMDB struct {
	Mdb memdb.MemDB
}

type Codec struct {
	X interface{}
}

type downloadTypes []string
type argumentError struct {
	msg string
}

func (e *argumentError) Error() string { return e.msg }

type decodeError struct {
	msg string
}

func (e *decodeError) Error() string { return e.msg }

// Create an encoder and send a value.
var enc = gob.NewEncoder(&replayBuffer)

// var dec = gob.NewDecoder(&replayBuffer)

var wg2 sync.WaitGroup
var fileMutex sync.Mutex
var csvWriter *csv.Writer
var xmlWriter *xml.Encoder
var replayBuffer bytes.Buffer
var openFiles, maxOpenFiles int
var bucket, prefix, region, profile, objectsdb, domain string
var validate, print, server bool
var downloads downloadTypes
var args arguments
var bool2int map[bool]int = map[bool]int{true: 1, false: 0}

func (dt *downloadTypes) String() string {
	return fmt.Sprint(*dt)
}

func (dt *downloadTypes) Set(value string) error {
	/* Uncomment to prevent multiple definitions of this parameter
	if len(*i) > 0 {
		return errors.New("[ERROR] Download types specified more than once\n")
	}
	*/
	for _, t := range strings.Split(value, ",") {
		*dt = append(*dt, t)
	}
	return nil
}

func handleArguments() (arguments, error) {
	flag.StringVar(&bucket, "bucket", "", "Specifies the bucket to use S3 objects")
	flag.StringVar(&prefix, "prefix", "", "Specifies the prefix to use S3 objects")
	flag.StringVar(&domain, "domain", "", "Specifies the domain to use SDB queries")
	flag.Var(&downloads, "download", "Saves the specified items to disk: version, object")
	flag.StringVar(&region, "region", "eu-west-1", "Specify which region to use")
	flag.BoolVar(&validate, "validate", false, "Process each S3Object for validity; Defaults to false")
	flag.IntVar(&maxOpenFiles, "maxfiles", 100, "Specify the number of concurrent files to write to")
	flag.BoolVar(&print, "print", false, "Print out Object information to stdout [NOT IMPLEMENTED]")
	flag.BoolVar(&server, "server", false, "Run as a web server with front-end interface [NOT IMPLEMENTED]")
	flag.StringVar(&profile, "profile", "", "Output profiling data to [FILE]")
	flag.StringVar(&objectsdb, "objectsdb", "./objects.db", "Save objects and heuristics to [FILE]")
	flag.Parse()

	// [NOTE]
	// This isn't really the correct way to handle parameters, or at least this isn't the most elegant solution.
	// Ideally I would have transitioned to using one of the more functionally complete and extensible flag handling packages
	// such as Kingpin or github.com/codegangsta/cli
	if len(bucket) == 0 {
		return args, &argumentError{"Falied to specify a bucket to use"}
	}
	if len(profile) == 0 {
		return args, &argumentError{"A valid filename must be specified for profile data"}
	}
	if len(domain) == 0 {
		return args, &argumentError{"Failed to specify a domain to use"}
	}

	args = arguments{items: map[string]interface{}{
		"bucket":    bucket,
		"prefix":    prefix,
		"downloads": downloads,
		"domain":    domain,
		"regions":   region,
		"validate":  validate,
		"maxfiles":  maxOpenFiles,
		"print":     print,
		"server":    server,
		"profile":   profile,
		"objectsdb": objectsdb}}
	return args, nil
}

type job struct {
	name     string
	duration time.Duration
	object   *s3.Object
	aB       *string
	svc      *s3.S3
	db       *sql.DB
	mdb      *memdb.MemDB
	w        *os.File
	csvFile  *os.File
	xmlFile  *os.File
}

type worker struct {
	id int
}

type Entry struct {
	XMLName   xml.Name `xml:"object"`
	ObjectId  string   `xml:"id"`
	VersionId string   `xml:"version"`
	Date      string   `xml:"modified"`
	Size      int64    `xml:"contentLength"`
}

// Usage:
// go run listObjects.go --bucket <bucket> --prefix <prefix> --download <comma separated list of items to download>
// [NOTE] This was correct at one point although exact functionality and parameters taken changed along the way as the script matured
func main() {

	args, e := handleArguments()

	if ae, ok := e.(*argumentError); ok {
		fmt.Println(ae.msg)
		return
	}

	if len(profile) > 0 {
		f, ef := os.Create(profile)
		if ef != nil {
			log.Fatal(ef)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	fmt.Printf("%q\n", args)

	// db, sqlErr := sql.Open("sqlite3", "./objects.db")
	db, sqlErr := sql.Open("mysql", "test:test@/objects")
	if sqlErr != nil {
		log.Fatal(sqlErr.Error())
		return
	}

	// INSERT_COLUMN_NAME_HERE signifies whichever column name we are working against on Amazon's SimpleDB
	res, sqlErr := db.Exec("SELECT 1 FROM `INSERT_COLUMN_NAME_HERE`")
	if res == nil {
		res, sqlErr = db.Exec(
			"CREATE TABLE `objects` (" +
				"`oid` VARCHAR(45)," +
				"`version` VARCHAR(33)," +
				"`data` BLOB NULL," +
				"`metadata` BLOB NULL," +
				"`islatest` INTEGER," +
				"`compressedsize` INTEGER," +
				"`size` INTEGER," +
				"`uid`  VARCHAR(100) NOT NULL," +
				"UNIQUE (`oid`, `version`)" +
				");")
		if sqlErr != nil {
			log.Fatal(sqlErr)
			return
		}

		res, sqlErr = db.Exec("CREATE INDEX objectByVersion on objects (oid, version)")
		if sqlErr != nil {
			log.Fatal(sqlErr)
			return
		}

		res, sqlErr = db.Exec(
			"CREATE TABLE `files` (" +
				"`oid` VARCHAR(45)," +
				"`version` VARCHAR(33)," +
				"`uid` VARCHAR(100) NOT NULL," +
				"`file` BLOB NULL," +
				"UNIQUE (`oid`, `version`)" +
				");")
		if sqlErr != nil {
			log.Fatal(sqlErr)
			return
		}

		res, sqlErr = db.Exec("CREATE INDEX fileByUID on files (uid)")
		if sqlErr != nil {
			log.Fatal(sqlErr)
			return
		}

	} else if sqlErr != nil {
		log.Fatal(sqlErr)
	}

	// [NOTE] 
	// MemDB functionality was never fully completed and for good reason - it simply wasn't needed, there are far better solutions (SQLite,MySQL, etc.)
	// Any remaining MemDB handling code is simply a holdover from previous iterations of the file.

	// Register S3Document and PersistentMDB interfaces 
	gob.Register((*S3Document)(nil))

	var w, csvFile, xmlFile *os.File
	var ferr, terr, merr error
	var mdb *memdb.MemDB

	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"S3Object": &memdb.TableSchema{
				Name: "S3Object",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.CompoundIndex{Indexes: []memdb.Indexer{&memdb.StringFieldIndex{Field: "S3DocumentID"}, &memdb.StringFieldIndex{Field: "S3VersionID"}}},
					},
				},
			},
		},
	}

	if _, err := os.Stat("./objects.enc"); err == nil {
		// Persistent store for mdb to be retrieved on next load
		// os.O_APPEND
		w, ferr = os.OpenFile("./objects.enc", os.O_RDWR|os.O_CREATE, 0644)
		if ferr != nil {
			log.Fatal(ferr)
		}
		mdb, merr = memdb.NewMemDB(schema)
		if merr != nil {
			log.Fatal(merr)
		}

		terr = ReplayMDB(w, mdb)
		if terr != nil {
			log.Fatal(terr)
		}
	} else {
		mdb, merr = memdb.NewMemDB(schema)
		if merr != nil {
			log.Fatal(merr)
		}

		// Persistent store for mdb to be retrieved on next load
		w, ferr = os.OpenFile("./objects.enc", os.O_RDWR|os.O_CREATE, 0644)
		if ferr != nil {
			log.Fatal(ferr)
		}
	}

	csvFile, csvFileErr := os.OpenFile("./objects.csv", os.O_RDWR|os.O_CREATE, 0644)
	if ferr != nil {
		log.Fatal(csvFileErr)
	}

	xmlFile, xmlFileErr := os.OpenFile("./objects.xml", os.O_RDWR|os.O_CREATE, 0644)
	if ferr != nil {
		log.Fatal(xmlFileErr)
	}

	csvWriter = csv.NewWriter(csvFile)
	xmlWriter = xml.NewEncoder(xmlFile)
	xmlWriter.Indent("  ", "    ")

	sess := session.New()
	svc := s3.New(sess, &aws.Config{Region: aws.String("eu-west-1")})
	SimpleDBService := simpledb.New(sess, &aws.Config{Region: aws.String("eu-west-1")})
	amazonBucket, amazonPrefix := args.items["bucket"].(string), args.items["prefix"].(string)

	fmt.Println("%v%v", SimpleDBService, svc)

	var aP, aB = &amazonPrefix, &amazonBucket
	fmt.Println(*aP, *aB)

	wg := &sync.WaitGroup{}
	// jobCh := make(chan job,2)
	jobCh := make(chan job)

	// Pool workers
	for i := 0; i < 3; i++ {
		wg.Add(1)
		w := worker{i}
		go func(w worker) {
			for j := range jobCh {
				// Here we do actual work processing elements off our queue
				w.process(j)
			}
			wg.Done()
		}(w)
	}

	// Modify INSERT_COLUMN_NAME_HERE to reflect whichever column names you happen to be working with...
	// [TODO] Could modify this to be somewhat more flexible but then we would neet to sanitise any incoming and untrusted data..
	query := "SELECT itemName() FROM `" + args.items["INSERT_COLUMN_NAME_HERE"].(string) + "` WHERE itemName() LIKE \"" + args.items["INSERT_COLUMN_NAME_HERE"].(string) + "%\""
	fmt.Println(query)

	sdbParams := &simpledb.SelectInput{
		SelectExpression: aws.String(query), // Required
		// ConsistentRead:   aws.Bool(true),
	}

	sdbErr := SimpleDBService.SelectPages(sdbParams, func(s *simpledb.SelectOutput, lastPage bool) (shouldContinue bool) {
		for _, obj := range s.Items {
			// log.Println(obj)
			definitionParams := &s3.GetObjectInput{
				Bucket: aB,       // Required
				Key:    obj.Name, // Required
			}

			// log.Println(version)
			wg2.Add(1)
			go inspectObject(definitionParams, svc)

			/*
				if dErr != nil {
				    if reqerr, ok := dErr.(awserr.RequestFailure); ok {
				        log.Println("Request failed",  reqerr.StatusCode(), reqerr.Code(), reqerr.Message(), reqerr.RequestID())
				    } else {
				        log.Println("Error:", dErr.Error())
				    }
				}
			*/
			// log.Println(definitionResp)
		}
		return true
	})

	if sdbErr != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(sdbErr.Error())
		return
	}

	// Pretty-print the response data.
	// fmt.Println(sdbResponse)

	/*
		err := svc.ListObjectsPages(&s3.ListObjectsInput{
			Bucket: aB,
			Prefix: aP,
		}, func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {

			for _, obj := range p.Contents {

				name := fmt.Sprintf("%s", *obj.Key)
				// Half a second
				duration := 500 * time.Millisecond
				// spawn job here
				jobCh <- job{name, duration, obj, aB, svc, db, mdb, w, csvFile, xmlFile}
				// time.Sleep(1 * time.Second)
			}
			return true
		})
	*/

	close(jobCh)
	wg.Wait()

	wg2.Wait()
	/*
		if err != nil {
			panic(err)
		}
	*/
}

func inspectObject(definitionParams *s3.GetObjectInput, svc *s3.S3) {

	_, dErr := svc.GetObject(definitionParams)

	if dErr != nil {
		if reqerr, ok := dErr.(awserr.RequestFailure); ok {
			log.Println("Request failed", reqerr.StatusCode(), reqerr.Code(), reqerr.Message(), reqerr.RequestID())
		} else {
			log.Println("Error:", dErr.Error())
		}
	}
	defer wg2.Done()
}

func (w worker) process(j job) {
	// log.Printf("worker%d: started %s, working for %fs\n", w.id, j.name, j.duration.Seconds())
	// log.Printf("[Worker %d] [Job %s] Object: %s", w.id, j.name, *j.object.Key)

	params := &s3.ListObjectVersionsInput{
		Bucket: j.aB, // Required
		Prefix: j.object.Key,
	}
	resp, err := j.svc.ListObjectVersions(params)

	if err != nil {
		log.Println(err.Error())
		return
	}

	var csvRecords [][]string
	//	var xmlRecords []Entry
	for _, version := range resp.Versions {
		// Uncomment this conditional to allow for all revisions of a given S3Document to be downloaded
		if *version.IsLatest == true {
			//fmt.Println("Reflection gave: ", reflect.TypeOf(version), reflect.TypeOf(obj))

			/*
				definitionParams := &s3.GetObjectInput{
					Bucket: j.aB, // Required
					Key:	j.object.Key,     // Required
					VersionId: version.VersionId,
				}

				// log.Println(version)
				// log.Printf("[Worker %d] [Job %s] Version: %s", w.id, j.name, *version.VersionId)
				// definitionResp, dErr := j.svc.GetObject(definitionParams)

				if dErr != nil {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
					log.Println(dErr.Error())
					return
				}

				// log.Printf("[Worker %2d] [Job %s] [Version %s] [Content-length %d]", w.id, j.name, *version.VersionId, *definitionResp.ContentLength)

				sdbParams := &simpledb.SelectInput{
				    SelectExpression: aws.String("SELECT itemName() FROM " + args.items["INSERT_COLUMN_NAME_HERE"].(string) + " WHERE "), // Required
				    // ConsistentRead:   aws.Bool(true),
				}
				sdbResponse, err := svc.Select(sdbParams)
			*/

			log.Printf("[Worker %2d] [Job %s] [Version %s] [Content-length %d]", w.id, j.name, *version.VersionId, *version.Size)

			entry := []string{*j.object.Key, *version.VersionId, strconv.FormatInt(*version.Size, 10), version.LastModified.String()}
			csvRecords = append(csvRecords, entry)
			xmlRecord := &Entry{ObjectId: *j.object.Key, VersionId: *version.VersionId, Date: version.LastModified.String(), Size: *version.Size}
			fileMutex.Lock()
			xmlWriter.Encode(xmlRecord)
			fileMutex.Unlock()

			// This code originally wrote out to an SQLite or MySQL DB
			// [NOTE] Originally this script was meant to support both modes of operation (writing to XML&CSV files along with to a Database store) on a per-paramater basis; Unfortunately due to a lack of spare time I had to eschew such functionality
			/*
				rb := new(bytes.Buffer)
				rb.ReadFrom(definitionResp.Body)
				data := rb

				var prb bytes.Buffer
				jerr := json.Indent(&prb, data.Bytes(), "", "\t")

				if jerr != nil {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
					log.Println(jerr.Error())
					continue
				}

				cs := compressStream(data.String())

				writeOutToDB(definitionResp, *j.object.Key, version, cs, j.db, prb)
			*/
			// writeOutToMDB(j.w, definitionResp, *j.object.Key, version, cs, j.mdb)
			break // Do not process any other entries for this S3Document (unset this along with the above conditional to allow processing of all revisions)
		}
	}
	fileMutex.Lock()
	csvWriter.WriteAll(csvRecords) // calls Flush internally
	fileMutex.Unlock()

	/*
		for k, xmlRecord := range xmlRecords {
			if err := enc.Encode(&xmlRecord); err != nil {
				log.Printf("[Key=%v] [Value=%v] error: %v\n", k, xmlRecord, err)
			}
		}
	*/
}

// Compress a given stream of bytes due to the potentiontally enormous file sizes we could be dealing with (Binary BLOBs etc.)
// [NOTE] Only ever used with SQLite and MySQL functionality
func compressStream(s string /* r io.ReadCloser */) *bytes.Buffer {
	var b bytes.Buffer

	gz, _ := gzip.NewWriterLevel(&b, 9)

	_, error := gz.Write([]byte(s))
	if error != nil {
		log.Println("[Operational error when writing to compressed stream]", error.Error())
	}

	gz.Close()

	return &b
}

// Restore all the data from the previously 'pickled' (apologies, Python termonology creeping in there) records.
func ReplayMDB(w *os.File, mdb *memdb.MemDB) error {

	var dec = gob.NewDecoder(w)
	var x Codec

	err := dec.Decode(&x)
	for ; err != io.EOF; err = dec.Decode(&x) {
		if err != nil {
			log.Println("[Unable to decode S3Document for MemoryDB store] ", err)
			continue
		}

		// fmt.Println(x, err)
		// obj := (*interface{}(x.X).(*S3Document))
		// Should never really happen, but it is nice to be thorough
		switch t := x.X.(type) {
		case *S3Document:
			// That's quite a cast! And most certainly not for the faint of heart!
			// return &((*interface{}(x.X).(*PersistentMDB)).Mdb), nil
			txn := mdb.Txn(true)

			s3obj := &(*interface{}(x.X).(*S3Document))
			if err := txn.Insert("S3Object", s3obj); err != nil {
				log.Println("[Unable to restore S3Document for MemoryDB store] ", err)
				continue
			}

			txn.Commit()

		default:
			// obj2 := (*interface{}(x.X).(*PersistentMDB))
			log.Println("[Type interface mismatch when restoring S3Document for MemoryDB store] Got type " + reflect.TypeOf(t).String())
		}

		// return &((*interface{}(x.X).(*PersistentMDB)).MDB)
		// return &obj.MDB
	}
	// [NOTE] Ideally we would want better error handling here! Again, this is holdover code that should not be used and has been left for posteriety
	return nil
}

// Write out all the object data to a 'pickled' file, containing any records we could parse successfuly.
func writeOutToMDB(w *os.File, r *s3.GetObjectOutput, obj string, version *s3.ObjectVersion, compressedStream *bytes.Buffer, mdb *memdb.MemDB) {

	s3obj := &S3Document{S3DocumentID: obj, S3VersionID: *version.VersionId, S3Document: compressedStream.Bytes(), S3DocumentMetadata: r.String(), S3DocumentLatest: *version.IsLatest, S3DocumentCompressedSize: int64(compressedStream.Len()), S3DocumentSize: *version.Size}

	txn := mdb.Txn(false)
	defer txn.Abort()

	raw, err := txn.First("S3Object", "id", obj, *version.VersionId)
	if err != nil {
		panic(err)
	}

	if raw != nil {
		return
	}

	txn = mdb.Txn(true)

	if err := txn.Insert("S3Object", s3obj); err != nil {
		panic(err)
	}
	txn.Commit()

	err = enc.Encode(Codec{s3obj})
	if err != nil {
		log.Println("[Operational error when encoding S3Document]", err)
	} else {
		replayReader := bytes.NewReader(replayBuffer.Bytes())

		// To facilitate safe concurrent writes
		var fMutex sync.Mutex
		fMutex.Lock()
		defer fMutex.Unlock()
		defer replayBuffer.Reset()

		_, ferr := io.Copy(w, replayReader)
		if ferr != nil {
			log.Println("[Operational error when writing S3Document to MemoryDB]", err)
		}
	}
}

func writeOutToDB(r *s3.GetObjectOutput, obj string, version *s3.ObjectVersion, compressedStream *bytes.Buffer, db *sql.DB, data bytes.Buffer) {

	uid := uuid.NewV4().String()

	_, sqlErr := db.Exec(
		"INSERT IGNORE INTO `objects` (oid, version, data, metadata, islatest, compressedsize, size, uid) values (?,?,?,?,?,?,?,?);",
		obj,
		version.VersionId,
		compressedStream.String(),
		r.String(),
		bool2int[*version.IsLatest],
		compressedStream.Len(),
		version.Size,
		uid,
	)
	if sqlErr != nil {
		log.Println("[Operational error during insertion into objects table] ", sqlErr.Error())
	}

	_, sqlErr = db.Exec(
		"INSERT IGNORE INTO `files` (oid, version, uid, file) values (?,?,?,?);",
		obj,
		version.VersionId,
		uid,
		data.String(),
	)
	if sqlErr != nil {
		log.Println("[Operational error during insertion into files table] ", sqlErr.Error())
	}

}

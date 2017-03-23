# golang-aws-s3-replicate

----
## Original purpose and development
Ideally, and back when I first began writing this utility, the intention was to create something to facilitate diagnosing and recovering various versions of a given S3 Document hosted on Amazon's AWS. 

Functionally speaking, it would execute a query on SimpleDB to find the desired object I was referencing, go through the paginated list of matching objects on the S3 Document store and archive them by some means. This started from simply storing each individual revision of an object's definition in a separate file, creating and replicating the directory as needed. This, however, proved problematic for two reasons: 

* Not particularly ideal given the underlying character set supported by whichever file-system was being used, including any maximum filename length limitations.
* Created potentially thousands of directory entries, making navigation impractical and sorting through them tedious.
* Became an organisational nightmare the more documents there were.

Consequently, I then tried tinkering around with a [MemoryDB store implementation](https://github.com/hashicorp/go-memdb), which, whilst interesting in itself did not really serve a practical purpose for the scope of this tool. For the purposes of posterity the incomplete logic for this code is still present.

With the next two iterations of the tool I introduced SQLite and MySQL as storage solutions. This particular functionality is complete and also resides in the code, albeit requiring some reshuffling and uncommenting.

Towards the end of what I would describe as actively developing the tool I also implemented CSV and XML handling, which from the code's current state seems to be the defacto method of archiving S3 Documents from a bucket. Note that the logic enforces downloading only the latest revision of an object definition (See the code comments for an idea of how to revert to storing multiple versions of a given document)

Ultimately I wanted to have many bells-and-whistles, implementing a graphical front-end to facilitate easier browsing (à la Amazon's own offering), retrieval of individual versions and so on. This eventually became a burden as the amount of spare time I could dedicate to writing this support tool became ever-increasingly limited, which itself resulted in a lot of hold-over code that still exists from previous iterations of the utility (Sadly there is no Git history associated with the repo as all development was done rather ad-hoc at the time).

----
## Nice features

This tool was designed to be be highly threaded and scalable from the ground-up. Evidence of such functionality exists within the worker-job-pool logic of the code. GoLang undoubtedly shines here, and as a measure of performance I once set the number of workers to be 50, the results of which were an unmitigated success - Naturally Amazon did not like such a spurious number of requests within such a small time frame and consequently began denying a certain portion of the requests being made. Hilariously satisfying :-)

----
## Not-so-nice code issues and caveats

The code suffers from a number of notable issues:

* Code bloat from the constant refactoring and removal of/addition of features.
* Not always idiomatic GoLang. For example error handling is not always done the Go way.
* NIHS (Not-invinted-here-syndrome). I tended to throw caution to the wind with my coding style and variable naming conventions (sometimes wildly inconsistent or completely arbitrary), although, as above this can be explained by the footnote below.
* Kitchen sink syndrome - This I believe is fairly self-explanatory.
* No source control. Forcing myself to use a Git repository from the get-go would have improved code organisation and would have also removed the feature creep induced by my own self-inflicted FUD (Fear-uncertainty-doubt) with regard to feature X or Y.
* The parameter handling is perhaps non-obvious and obtuse. This grew from the nature of the project and me being the only person to develop and use the tool.

Admittedly, some of these issues arose as a product of me learning GoLang at the time, slowly gleaning pieces of syntax here and there, consulting the API & Language documentation, becoming frustrated at myself for not having consulted said API & Language documentation beforehand and a general time-pressure.

----
## Future development

Given that there is very little impetus for me to use the tool currently I doubt there will be much in the way of development. After all, this was both an exercise in learning GoLang and also writing something moderately useful :-)

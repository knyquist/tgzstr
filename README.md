# tgzstr

This script takes a stdin stream that's a ghostzipped tarball, unzips and untars it, and sends it up to S3. It makes it easy to grab a tgz file from somewhere on the internet and directly send the unzipped/untarred files to S3 without having to hit your file system. Good for big files, knomsan?

This script is a result of a nerdbombing session with M. Souza the day before my bachelor party, August 16, 2018.
# apply fractions to AADT data

Extracted from grid_merge.

# How to generate summations of HPMS+Detector-based

Assuming all of the prerequisites are completed, you need to run the
app.js function as follows:

```
node app.js -y 2012 -j 6 --recheck --area county > county.2012.out 2>&1
```

In this case, the year is 2012, the concurrency of node processes is
set to 6 (not the same as parallel jobs, but similar), the recheck
parameter is set to true, and the type of area to process is set to
county.

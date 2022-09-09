library(Seurat)

args = commandArgs(trailingOnly=TRUE)

print(sprintf("working with %s samples", args[1]))
number_to_merge <- args[1]

scdata_list <- list()

print("------------")
print(Sys.time())
print("started reading object")
print("------------")

object <- readRDS("r.rds")

print("------------")
print(Sys.time())
print("finished reading object")
print("------------")
 

for(i in 1:number_to_merge) {
	object <- RenameCells(object, new.names = paste0(i, colnames(object)))
	scdata_list[[i]] <- object
}

print("------------")
print(Sys.time())
print("Finished creating object list")
print("Starting merge")
print("------------")

# print("scdata_listDebug")
# print(str(scdata_list))

scdata <- merge(scdata_list[[1]], y = scdata_list[-1])

print("------------")
print(Sys.time())
print("Finished merge")
print("------------")

print(scdata)

print("------------")
print(Sys.time())
print("Finished running successfully")
print("------------")

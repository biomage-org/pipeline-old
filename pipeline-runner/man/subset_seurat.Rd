% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/subset-1-subset_seurat.R
\name{subset_seurat}
\alias{subset_seurat}
\title{create a subset experiment}
\usage{
subset_seurat(input, pipeline_config, prev_out = NULL)
}
\arguments{
\item{input}{list containing:
\itemize{
\item parentExperimentId character
\item subsetExperimentId character
\item cellSetKeys character vector of cellset keys to subset
\item experimentName character
\item parentProcessingConfig The processingConfig of the parent experiment
}}

\item{pipeline_config}{list}

\item{prev_out}{list, ignored because this is the first step in the subset pipeline}
}
\value{
list containing scdata_list, annotations and sample_id_map
}
\description{
This is the first step of a subset pipeline, which basically takes the parent
experiment ID and cellset keys to keep as input, extracts the cell ids to keep
and subsets and slims down the parent seurat object.
}

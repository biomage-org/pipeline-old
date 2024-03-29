% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/qc-6-subsample-geosketch.R
\name{run_geosketch}
\alias{run_geosketch}
\title{Perform geometric sketching}
\usage{
run_geosketch(scdata, dims, perc_num_cells, reduction = "pca")
}
\arguments{
\item{dims}{Number of dimensions of PC space to use}

\item{object}{Seurat object}

\item{num_cells}{Number of desired cells}
}
\value{
Seurat object downsampled to desired number of cells
}
\description{
See https://github.com/brianhie/geosketch
}

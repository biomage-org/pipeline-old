% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/gem2s-7-upload_to_aws.R
\name{build_sample_cellsets}
\alias{build_sample_cellsets}
\title{Create cell set using Seurat samples information}
\usage{
build_sample_cellsets(input, scdata_list, color_pool)
}
\arguments{
\item{input}{The input object from the request}

\item{scdata_list}{list of Seurat objects}

\item{color_pool}{list of colors to use}
}
\value{
cell set filled with samples information
}
\description{
Create cell set using Seurat samples information
}
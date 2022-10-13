library(mockery)
library(withr)
library(testthat)
library(zeallot)
library(tryCatchLog)

stub_s3_list_objects <- function(Bucket, Prefix) {

  # this workaround is the lesser evil ("bucket/./project/sample")
  Prefix <- gsub("^./", "", Prefix)

  # returns list with structure like s3$list_objects, but with mocked paths
  files <- list.files(file.path(Bucket, Prefix), full.names = TRUE, recursive = TRUE)
  l <- as.list(files)
  names(l) <- NULL
  l2 <- lapply(l, list)

  for (i in seq_along(l2)) {
    names(l2[[i]]) <- "Key"
  }
  list(Contents = l2)
}

stub_s3_get_object <- function(Bucket, Key) {
  # returns a list of the raw file read from the mocked s3 bucket.
  out <- list(body = readBin(file.path(Bucket, Key),
                             what = "raw", n = 120000000L),
              rest = list())
  return(out)
}

stub_file.path <- function(...) {
  file.path(".", ...)
}

stubbed_download_user_files <- function(input, pipeline_config, prev_out = list()) {
  # helper to simplify calls to the stubbed function

  mockedS3 <- list(
    list_objects=stub_s3_list_objects,
    get_object=stub_s3_get_object
  )

  # where makes sure where we are stubbing the what calls.
  mockery::stub(where = download_user_files, what = "paws::s3", how = mockedS3)
  mockery::stub(get_gem2s_file, "s3$list_objects", mockedS3$list_objects)

  mockery::stub(download_user_files, "file.path", stub_file.path)
  mockery::stub(get_gem2s_file, "file.path", stub_file.path)

  mockery::stub(download_and_store, "s3$get_object", mockedS3$get_object)

  res <- download_user_files(input, pipeline_config, input_dir = "./input", prev_out = prev_out)
  # download_user_files creates a "/input" folder in the pod. defer deleting
  # it during tests.
  withr::defer(unlink("./input", recursive = TRUE), envir = parent.frame())

  res
}

stub_put_object_in_s3 <- function(pipeline_config, bucket, object, key){

}


test_that("gem2s-1 - donwload files downloads 10x files", {
  base_path <- ifelse(basename(getwd()) == "pipeline-runner",
                      "./tests/testthat",
                      ".")

  mock_path <- file.path(base_path,
                         "mock_data")

  input <-
    RJSONIO::fromJSON(file.path(mock_path, "input/input.json"))

  pipeline_config <-
    list(originals_bucket = file.path(mock_path, "mock_bucket"))

  res <- stubbed_download_user_files(input, pipeline_config)

  for (sample in dir("./input", full.names = T)) {
    for (file in dir(sample, full.names = T)) {
      expect_snapshot_file(file,
                           name = paste(basename(sample),
                                        basename(file), sep = "-"))
    }
  }
  out_path <- file.path(base_path, "out.rds")

  saveRDS(res, out_path)
  expect_snapshot_file(out_path, name = paste("gem2s-1", basename(out_path), sep = "-"))
  defer_parent(unlink(out_path))

})


test_that("gem2s-2 - load_user_files loads mocked files", {
  base_path <- ifelse(basename(getwd()) == "pipeline-runner",
                      "./tests/testthat",
                      ".")

  mock_path <- file.path(base_path,
                         "mock_data")

  input <-
    RJSONIO::fromJSON(file.path(mock_path, "input/input.json"))

  prev_out <-
    readRDS(file.path(base_path, "_snaps/pipeline", "gem2s-1-out.rds"))
  prev_out <- prev_out$output

  experiment_path <- file.path(base_path, prev_out$config$name)
  local_file(experiment_path)
  dir.create(experiment_path)

  for (sample_id in prev_out$config$samples) {
    dir.create(file.path(experiment_path, sample_id))
    for (sample_file in dir(
      file.path(base_path, "_snaps/pipeline"),
      pattern = sample_id,
      full.names = TRUE
    )) {
      sample_file_name <-
        file.path(experiment_path, sample_id, gsub(paste0(sample_id, "-"), "", basename(sample_file)))
      file.copy(sample_file, sample_file_name)
    }
  }

  res <-
    load_user_files(input, NULL, prev_out, input_dir = experiment_path)

  out_path <- file.path(base_path, "out.rds")
  saveRDS(res, out_path)
  expect_snapshot_file(out_path, name = paste("gem2s-2", basename(out_path), sep = "-"))
  defer_parent(unlink(out_path))

})


test_that("gem2s-3 - runs empty drops", {
  base_path <- ifelse(basename(getwd()) == "pipeline-runner",
                      "./tests/testthat",
                      ".")

  mock_path <- file.path(base_path,
                         "mock_data")

  input <-
    RJSONIO::fromJSON(file.path(mock_path, "input/input.json"))

  prev_out <-
    readRDS(file.path(base_path, "_snaps/pipeline", "gem2s-2-out.rds"))
  prev_out <- prev_out$output

  res <- run_emptydrops(input, NULL, prev_out)
  out_path <- file.path(base_path, "out.rds")
  saveRDS(res, out_path)
  expect_snapshot_file(out_path, name = paste("gem2s-3", basename(out_path), sep = "-"))
  defer_parent(unlink(out_path))

})


test_that("gem2s-4 - scores doublets", {
  base_path <- ifelse(basename(getwd()) == "pipeline-runner",
                      "./tests/testthat",
                      ".")

  mock_path <- file.path(base_path,
                         "mock_data")

  input <-
    RJSONIO::fromJSON(file.path(mock_path, "input/input.json"))

  prev_out <-
    readRDS(file.path(base_path, "_snaps/pipeline", "gem2s-3-out.rds"))
  prev_out <- prev_out$output

  res <- score_doublets(input, NULL, prev_out)
  out_path <- file.path(base_path, "out.rds")
  saveRDS(res, out_path)
  expect_snapshot_file(out_path, name = paste("gem2s-4", basename(out_path), sep = "-"))
  defer_parent(unlink(out_path))

})



test_that("gem2s-5 - creates seurat objects", {
  base_path <- ifelse(basename(getwd()) == "pipeline-runner",
                      "./tests/testthat",
                      ".")

  mock_path <- file.path(base_path,
                         "mock_data")

  input <-
    RJSONIO::fromJSON(file.path(mock_path, "input/input.json"))

  prev_out <-
    readRDS(file.path(base_path, "_snaps/pipeline", "gem2s-4-out.rds"))
  prev_out <- prev_out$output

  res <- create_seurat(input, NULL, prev_out)
  out_path <- file.path(base_path, "out.rds")
  saveRDS(res, out_path)
  expect_snapshot_file(out_path, name = paste("gem2s-5", basename(out_path), sep = "-"))
  defer_parent(unlink(out_path))

})


test_that("gem2s-6 - prepares experiment", {
  base_path <- ifelse(basename(getwd()) == "pipeline-runner",
                      "./tests/testthat",
                      ".")

  mock_path <- file.path(base_path,
                         "mock_data")

  input <-
    RJSONIO::fromJSON(file.path(mock_path, "input/input.json"))

  prev_out <-
    readRDS(file.path(base_path, "_snaps/pipeline", "gem2s-5-out.rds"))
  prev_out <- prev_out$output

  res <- prepare_experiment(input, NULL, prev_out)
  out_path <- file.path(base_path, "out.rds")
  saveRDS(res, out_path)
  expect_snapshot_file(out_path, name = paste("gem2s-6", basename(out_path), sep = "-"))
  defer_parent(unlink(out_path))

})

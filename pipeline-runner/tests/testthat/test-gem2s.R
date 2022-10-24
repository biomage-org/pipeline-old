stub_s3_list_objects <- function(Bucket, Prefix) {
  # this workaround is the lesser evil ("bucket/./project/sample")
  Prefix <- gsub("^./", "", Prefix)

  # returns list with structure like s3$list_objects, but with mocked paths
  files <-
    list.files(file.path(Bucket, Prefix),
               full.names = TRUE,
               recursive = TRUE)
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

stubbed_download_user_files <-
  function(input, pipeline_config, prev_out = list()) {
    # helper to simplify calls to the stubbed function

    mockedS3 <- list(list_objects = stub_s3_list_objects,
                     get_object = stub_s3_get_object)

    # where makes sure where we are stubbing the what calls.
    mockery::stub(where = download_user_files,
                  what = "paws::s3",
                  how = mockedS3)
    mockery::stub(get_gem2s_file, "s3$list_objects", mockedS3$list_objects)

    mockery::stub(download_user_files, "file.path", stub_file.path)
    mockery::stub(get_gem2s_file, "file.path", stub_file.path)

    mockery::stub(download_and_store, "s3$get_object", mockedS3$get_object)

    res <-
      download_user_files(input,
                          pipeline_config,
                          input_dir = "./input",
                          prev_out = prev_out)
    # download_user_files creates a "/input" folder in the pod. defer deleting
    # it during tests.
    withr::defer(unlink("./input", recursive = TRUE), envir = parent.frame())

    res
  }

stub_put_object_in_s3 <-
  function(pipeline_config, bucket, object, key) {
    if (!dir.exists(bucket))
      dir.create(bucket)
    writeBin(object, file.path(bucket, key))
  }

stub_remove_bucket_folder <-
  function(pipeline_config, bucket, folder) {
    # we cleanup inside the corresponding tests
  }

stub_tempdir <- function() {
  # stub to write always to the same place and be able to capture written files
  # consistently
  base_path <- ifelse(basename(getwd()) == "pipeline-runner",
                      "./tests/testthat",
                      ".")

  mock_path <- file.path(base_path,
                         "mock_data")

  temp_path <- file.path(mock_path, "temp")

  if (!dir.exists(temp_path))
    dir.create(temp_path, recursive = T)

  return(temp_path)
}

stub_put_object_in_s3_multipart <-
  function(pipeline_config, bucket, object, key) {
    # if we do not test the raw RDSs uploaded by upload_to_aws, we can remove
    # this stub function
    dir_path <- file.path(bucket, dirname(key))
    if (!dir.exists(dir_path))
      dir.create(dir_path, recursive = T)
    file.copy(object, file.path(bucket, key))
  }

stubbed_upload_to_aws <-
  function(input, pipeline_config, prev_out) {
    mockery::stub(upload_to_aws, "put_object_in_s3", stub_put_object_in_s3)
    mockery::stub(upload_to_aws,
                  "remove_bucket_folder",
                  stub_remove_bucket_folder)
    mockery::stub(upload_to_aws, "tempdir", stub_tempdir)
    mockery::stub(upload_to_aws,
                  "put_object_in_s3_multipart",
                  stub_put_object_in_s3_multipart)

    upload_to_aws(input, pipeline_config, prev_out)

  }

mock_pipeline_config <-
  function(development_aws_server = "mock_aws_server") {
    local_envvar(list("AWS_ACCOUNT_ID" = "000000000000",
                      "ACTIVITY_ARN" = "mock_arn"))

    pipeline_config <- load_config(development_aws_server)

    base_path <- ifelse(basename(getwd()) == "pipeline-runner",
                        "./tests/testthat",
                        ".")


    mock_path <- file.path(base_path,
                           "mock_data")

    # replace buckets with the local path
    for (bucket in grep("bucket$", names(pipeline_config), value = T)) {
      pipeline_config[[bucket]] <- file.path(mock_path, bucket)
    }

    return(pipeline_config)

  }


load_experiment_input <- function(mock_data_path, experiment_id) {
  RJSONIO::fromJSON(file.path(
    mock_data_path,
    "input",
    paste(experiment_id, "input.json", sep = "-")
  ))

}


path_setup <- function() {
  base_path <- ifelse(basename(getwd()) == "pipeline-runner",
                      "./tests/testthat",
                      ".")

  mock_path <- file.path(base_path,
                         "mock_data")

  snaps_path <- file.path(base_path, "_snaps")

  return(list(base = base_path, mock_data = mock_path, snaps = snaps_path))

}

make_snapshot_name <- function(step_n, experiment_id, output_name) {
  paste("gem2s", step_n, experiment_id, paste0(output_name, ".R"), sep = "-")
}


source_prev_out <- function(paths, current_step, experiment_id, output_name = "out") {
  source(file.path(paths$snaps,
                   "gem2s",
                   make_snapshot_name(current_step - 1,
                                      experiment_id,
                                      output_name)),
         local = parent.frame())
}

step_setup <- function(step_n, experiment_id) {

  paths <- path_setup()
  input <-
    load_experiment_input(paths$mock_data, experiment_id)
  pipeline_config <- mock_pipeline_config()

  output <-  list(paths = paths,
                   input = input,
                   pipeline_config = pipeline_config
                   )

  if (step_n > 1) {
    source_prev_out(paths, step_n, experiment_id)
    output[["prev_out"]] <-  res$output
  }
  return(output)
}

test_gem2s <- function(experiment_id) {

  test_that("gem2s-1 - donwload files downloads 10x files", {

    step_n <- 1
    setup <- step_setup(step_n, experiment_id)

    res <- stubbed_download_user_files(setup$input, setup$pipeline_config)

    # snapshot downloaded files
    for (sample in dir("./input", full.names = T)) {
      for (file in dir(sample, full.names = T)) {
        expect_snapshot_file(file,
                             name = paste(basename(sample),
                                          basename(file), sep = "-"))
      }
    }

    # snapshot download_user_files output
    snapshot_name <- make_snapshot_name(step_n, experiment_id, "out")
    withr::with_tempfile("tf", {
      dump("res", tf)
      expect_snapshot_file(tf, name = snapshot_name)
    })
  })


  test_that("gem2s-2 - load_user_files loads mocked files", {

    step_n <- 2
    setup <- step_setup(step_n, experiment_id)

    # need to download files before loading them
    stubbed_download_user_files(setup$input, setup$pipeline_config)
    input_path <- file.path(setup$paths$base, "input")

    res <- load_user_files(setup$input, NULL, setup$prev_out, input_path)

    # snapshot load_user_files output
    snapshot_name <- make_snapshot_name(step_n, experiment_id, "out")
    withr::with_tempfile("tf", {
      dump("res", tf)
      expect_snapshot_file(tf, name = snapshot_name)
    })

  })


  test_that("gem2s-3 - runs empty drops", {

    step_n <- 3
    setup <- step_setup(step_n, experiment_id)

    res <- run_emptydrops(setup$input, NULL, setup$prev_out)

    # snapshot run_emptydrops output
    snapshot_name <- make_snapshot_name(step_n, experiment_id, "out")
    withr::with_tempfile("tf", {
      dump("res", tf)
      expect_snapshot_file(tf, name = snapshot_name)
    })
  })


  test_that("gem2s-4 - scores doublets", {

    step_n <- 4
    setup <- step_setup(step_n, experiment_id)

    res <- score_doublets(setup$input, NULL, setup$prev_out)

    # snapshot score_doublets output
    snapshot_name <- make_snapshot_name(step_n, experiment_id, "out")
    withr::with_tempfile("tf", {
      dump("res", tf)
      expect_snapshot_file(tf, name = snapshot_name)
    })
  })


  test_that("gem2s-5 - creates seurat objects", {

    step_n <- 5
    setup <- step_setup(step_n, experiment_id)

    res <- create_seurat(setup$input, NULL, setup$prev_out)

    # snapshot create seurat output
    snapshot_name <- make_snapshot_name(step_n, experiment_id, "out")
    withr::with_tempfile("tf", {
      dump("res", tf)
      expect_snapshot_file(tf, name = snapshot_name)
    })
  })


  test_that("gem2s-6 - prepares experiment", {

    step_n <- 6
    setup <- step_setup(step_n, experiment_id)

    res <- prepare_experiment(setup$input, NULL, setup$prev_out)

    snapshot_name <- make_snapshot_name(step_n, experiment_id, "out")
    withr::with_tempfile("tf", {
      dump("res", tf)
      expect_snapshot_file(tf, name = snapshot_name)
    })

    # snapshot qc_config independently as it is used frequently in QC tests
    qc_config <- res$output$qc_config
    qc_config_snapshot_name <- make_snapshot_name(step_n, experiment_id, "qc_config")
    withr::with_tempfile("tf_qc_config", {
      dump("qc_config", tf_qc_config)
      expect_snapshot_file(tf_qc_config, name = qc_config_snapshot_name)
    })


  })


  test_that("gem2s-7 - creates cellsets and uploads to AWS", {

    step_n <- 7
    setup <- step_setup(step_n, experiment_id)

    res <- stubbed_upload_to_aws(setup$input, setup$pipeline_config, setup$prev_out)

    # cellsets file
    cellset_snapshot_name <- make_snapshot_name(step_n, experiment_id, "cellsets.json")
    expect_snapshot_file(
      file.path(setup$pipeline_config$cell_sets_bucket, experiment_id),
      name = cellset_snapshot_name
    )
    withr::defer(unlink(setup$pipeline_config$cell_sets_bucket, recursive = TRUE),
                 envir = parent.frame())


    # raw sample seurat objects. no way to use text representations. we could not
    # test them at all, they are just copies of the previous seurat objects

    # for (sample_id in prev_out$config$samples) {
    #   expect_snapshot_file(
    #     file.path(
    #       pipeline_config$source_bucket,
    #       experiment_id,
    #       sample_id,
    #       "r.rds"
    #     ),
    #     name = paste("gem2s-7", experiment_id, sample_id, "r.rds", sep = "-")
    #   )
    # }
    withr::defer(unlink(setup$pipeline_config$source_bucket, recursive = T))
    withr::defer(unlink(file.path(setup$paths$mock_data, "temp"), recursive = T))

  })
}

test_gem2s("mock_experiment_id")

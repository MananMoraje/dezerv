library(readxl)
library(writexl)

# Define the function
sum_columns <- function(data1_path, data2_path, output_path) {
  # Load the Excel files without headers
  data1 <- read_excel(data1_path, col_names = FALSE)
  data2 <- read_excel(data2_path, col_names = FALSE)

  # Check if the data frames have the same number of rows
  if (nrow(data1) != nrow(data2)) {
    stop("Data1 and Data2 must have the same number of rows")
  }

  # Sum the corresponding columns
  result <- data.frame(
    SumA = data1[[1]] + data2[[1]],
    SumB = data1[[2]] + data2[[2]]
  )

  # Write the result to a new Excel file
  write_xlsx(result, output_path)
}

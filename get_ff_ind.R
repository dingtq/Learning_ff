############################################################################
# Small program to fetch and organize Fama-French industry data.
# The idea is to make a table that could be used for SQL merges.
############################################################################

library(DBI)
library(dplyr, warn.conflicts = FALSE)
library(tidyr)
library(readr)

# The URL for the data.
get_ff_ind <- function(num = 48) {
  t <- tempfile(fileext = ".zip") 
  
  base_url <- paste0("https://mba.tuck.dartmouth.edu/",
                     "pages/faculty/ken.french/ftp/Siccodes")
  url <- paste0(base_url, num, ".zip")
  
  download.file(url, t)
  
  ff_data <- 
    readr::read_fwf(t, 
                    col_positions = readr::fwf_widths(c(3, 7, NA),
                                                      c("ff_ind", 
                                                        "ff_ind_short_desc", 
                                                        "sic_range")),
                    col_types = "icc") %>%
    mutate(ff_ind_desc = if_else(!is.na(ff_ind_short_desc), 
                                 sic_range, NA_character_)) %>%
    tidyr::fill(ff_ind, ff_ind_short_desc, ff_ind_desc) %>%
    filter(grepl("^[0-9]", sic_range)) %>%
    tidyr::extract(sic_range, 
                   into = c("sic_min", "sic_max", "sic_desc"),
                   regex = "^([0-9]+)-([0-9]+)(.*)$",
                   convert = TRUE) %>%
    mutate(sic_desc = trimws(sic_desc))
  unlink(t)
  ff_data
}

write_ff_table <- function(i) {
  pg <- dbConnect(RPostgres::Postgres())
  
  dbExecute(pg, "SET search_path TO ff")
  
  get_ff_ind(i) %>% 
    dbWriteTable(pg, paste0("ind_", i), .,
                 overwrite=TRUE, row.names=FALSE)
  
  rs <- dbExecute(pg, paste0("VACUUM ind_", i))
  rs <- dbExecute(pg, paste0("CREATE INDEX ON ind_", i,
                              " (ff_ind)"))
  
  dbExecute(pg, paste0("ALTER TABLE ind_", i, " OWNER TO ff"))
  dbExecute(pg, paste0("GRANT SELECT ON ind_", i, " TO ff_access"))
  
  sql <- paste0("
    COMMENT ON TABLE ind_", i, " IS
    'CREATED USING get_ff_ind.R ON ", Sys.time() , "';")
  rs <- dbExecute(pg, paste(sql, collapse="\n"))
  rs <- dbDisconnect(pg)
}

# Get Fama-French industry data
lapply(c(12, 17, 48, 49L), write_ff_table)


############################################################################
# Small program to import and clean up a dataset from SDC M&A database.
############################################################################
library(dplyr, warn.conflicts = FALSE)
library(readr)
library(lubridate)
library(ggplot2)

col_names <- c("date_announced", "date_effective", "target_name",
               "target_nation", "acquirer_name", "acquirer_nation", "status",
               "pct_of_shares_acq", "pct_owned_after_transaction",
               "acquirer_cusip", "target_cusip", "value_of_transaction_mil",
               "acquirer_prior_mktval", "target_prior_mktval",
               "acq_nation_code", "target_nation_code")

ma_sdc <-
  read_fwf("ye2020-12-22_2010to2018.txt",
           skip = 8,
           locale = locale(date_format = "%m/%d/%y",
                           grouping_mark = ",",
                           decimal_mark = "."),
           col_positions = readr::fwf_widths(c(13, 13, 40, 19, 37, 15, 14, 12,
                                               12, 12, 8, 19, 21, 23, 10, NA),
                                             col_names),
           col_types = "cccccccddccccccc") %>%
  filter(!is.na(date_announced)) %>%
  mutate(date_announced = mdy(date_announced),
         date_effective = mdy(date_effective),
         value_of_transaction_mil = parse_number(value_of_transaction_mil),
         acquirer_prior_mktval = parse_number(acquirer_prior_mktval),
         target_prior_mktval = parse_number(target_prior_mktval)) %>%
  head(-1)

## 1. We use 'skip = 8' here to skip the first 8 lines, which are basically messy misaligned column names. 
##    Instead, we manually code column names into `col_names`. Otherwise, they will cause many parse failures.
## 2. To get `fwf_widths`, we can use the Excel GUI by: First, open an blank Microsoft Excel; Then click 
##    'view'-> 'Macros'-> 'Record Macro'->'OK'; Then open the text file in Excel, and a 'Text Import Wizard' 
##    window will start. Follow the steps to set column width and data format, and click 'Finish'. We 
##    can first confirm that the first 8 lines are only messy column names. Then click 'view'-> 'Macros'-> 
##     'Stop Recording'->'View Macros'->'Edit', Here we can see the starting position of each column as 0, 13, 
##    26, 66, ...And the moving difference are the widths we should put in fwf_widths(c()). It seems a bit 
##    complicated, but more efficient and accurate than manual count if we have many columns to import.
## 3. The code will get parsing failure warning which relates to the last line, which we use head(-1) to remove.

# Show the dataset, get some basic descriptives and visualization.
ma_sdc

ma_sdc %>% count(acquirer_nation, acq_nation_code)

ma_sdc %>% count(target_nation) %>% arrange(desc(n))

ma_sdc %>% count(target_nation, target_nation_code) %>% arrange(desc(n))

ma_sdc %>% count(annc_year = year(date_announced))

ma_sdc %>% count(eff_year = year(date_effective))

ma_sdc %>%
  ggplot(aes(pct_of_shares_acq)) +
  geom_histogram(binwidth = 1)

#  It is weird to see that the histogram shows percentage of shares acquired from 0 to 200%. Let's check out why.
summary(ma_sdc$pct_of_shares_acq)

summary(ma_sdc$pct_owned_after_transaction)

ma_sdc %>% filter(pct_of_shares_acq > 100)

ma_sdc %>%
  filter(pct_owned_after_transaction > 100) %>%
  select(target_name, pct_of_shares_acq, pct_owned_after_transaction)

# Lastly, we filter out this weird case and show the correct histogram figure.
ma_sdc %>%
  filter(pct_of_shares_acq <= 100 & pct_owned_after_transaction <= 100) %>%
  ggplot(aes(pct_of_shares_acq)) +
  geom_histogram(binwidth = 1)


############################################################################
# Small program to import "Money Left on the Table in IPOs by Firm" data
# from Jay R. Ritter's website 
# (https://site.warrington.ufl.edu/ritter/files/money-left-on-the-table.pdf)
############################################################################
library(dplyr, warn.conflicts = FALSE)

# Strictly speaking, the next four lines are not needed, because the functions
# we use from these packages are fully qualified (e.g., "readr::read_lines"). 
# But these lines will throw an error if the packages---which we need---are 
# not installed.
library(pdftools)
#> Using poppler version 0.73.0
library(readr)
library(tidyr)
library(lubridate)
#> 
#> Attaching package: 'lubridate'
#> The following objects are masked from 'package:base':
#> 
#>     date, intersect, setdiff, union

url <- "https://site.warrington.ufl.edu/ritter/files/money-left-on-the-table.pdf"
t <- tempfile() 
download.file(url, t)

# The second column ("company") is challenging because it contains spaces, but
# isn't "quoted". Fortunately, this is the only column with embedded spaces and
# it is followed by a column ("ipo_date") that is strictly six digits (yymmdd).
# We can use this to effectively "delimit" the "company" column from the rest of
# the data.
regex <- paste0("^([^\\s]+)\\s+",   # Non-space characters  (followed by spaces)
                "(.*?)\\s+",        # Any characters, which may include spaces 
                #   (followed by spaces)
                # Note use of "?" to make capturing 
                #   "non-greedy". What happens if it is 
                #   removed?
                #
                "([0-9]{6})\\s+",   # Six digits (followed by spaces)
                "([^\\s]+)\\s+",    # Non-space characters (followed by spaces)
                "([^\\s]+)\\s+",    # Non-space characters (followed by spaces)
                "([^\\s]+)\\s+",    # Non-space characters (followed by spaces)
                "(.*)$")            
cols <-  c("amount_left_on_table", "company", "ipo_date", 
           "offer_price", "first_close_price", "shares_offered", "ticker")

ritter_data <-
  pdftools::pdf_text(url) %>% 
  # From visual inspection, we can see that it's not till the 48th row that
  # we get to tabular data.
  readr::read_lines(skip = 47) %>%
  # We read the whole data set into a single-column data frame
  tibble(temp = .) %>%
  # Here we use the regular expression to split the data into columns
  tidyr::extract(temp, cols, regex) %>%
  # Finally, fix up the data types of the columns
  mutate(across(all_of(c("amount_left_on_table", "first_close_price",
                         "offer_price", "shares_offered")), 
                readr::parse_number),
         ipo_date = lubridate::ymd(ipo_date)) 
ritter_data

############################################################################
# Small program to import the Internet Appendix Table OA.1 of 
# "The Customer Knows Best: The Investment Value of Consumer Opinions"
# (http://jfe.rochester.edu/Huang_app.pdf) 
############################################################################

library(dplyr, warn.conflicts = FALSE)

# Strictly speaking, the next four lines are not needed, because the functions
# we use from these packages are fully qualified (e.g., "readr::read_lines"). 
# But these lines will throw an error if the packages---which we need---are 
# not installed.
library(pdftools)
#> Using poppler version 0.73.0
library(readr)
library(tidyr)
library(lubridate)
#> 
#> Attaching package: 'lubridate'
#> The following objects are masked from 'package:base':
#> 
#>     date, intersect, setdiff, union

url <- "http://jfe.rochester.edu/Huang_app.pdf"
t <- tempfile() 
download.file(url, t)

regex <- paste0("^\\s?(.*?)\\s+",   # Any characters, which may include spaces 
                #   (preceded by and followed by spaces)
                "([^\\s]+)\\s+",             # Non-space characters
                "([0-9]{1,2}/[0-9]{4})\\s+", # Date in m/yyyy format
                "([0-9]{1,2}/[0-9]{4})\\s+", # Date in m/yyyy format
                "([^\\s]+)\\s+",             # Non-space characters
                "(.*)$")                     # The remaining characters

cols <-  c("company_name", "industry", "start", 
           "end", "months", "reviews")

huang_data <-
  pdftools::pdf_text(url) %>% 
  # From visual inspection, we can see that it's not till the 26th row that
  # we get to tabular data. And there are just 352 rows of such data.
  readr::read_lines(skip = 26, n_max = 352) %>%
  tibble(temp = .) %>%
  # Here we use the regular expression to split the data into columns
  tidyr::extract(temp, cols, regex) %>%
  # Remove rows without data
  filter(!is.na(company_name)) %>%
  mutate(across(all_of(c("months", "reviews")), 
                readr::parse_number)) %>%
  mutate(across(all_of(c("start", "end")), 
                ~ dmy(paste0("1/", .x))))

huang_data

``` r
############################################################################
# Small program to import Historical GICS data from MSCI website
# (https://www.msci.com/gics)
############################################################################
library(DBI)
library(dplyr, warn.conflicts = FALSE)
library(tidyr)
library(readr)

# Task 1. Get GICS URLs
# There are multiple versions of GICS from the following URLs. Develop a function,
# which takes the Effective Until date and outputs a corresponding URL.
# https://www.msci.com/documents/10199/4547797/Effective+From+1999+Until+March+28%2C+2002.xls
# https://www.msci.com/documents/10199/4547797/Effective+Until+April+30%2C+2003.xls
# https://www.msci.com/documents/10199/4547797/Effective+Until+April+30%2C+2004.xls
# https://www.msci.com/documents/10199/4547797/Effective+Until+April+29%2C+2005.xls
# https://www.msci.com/documents/10199/4547797/Effective+Until+April+28%2C+2006.xls
# https://www.msci.com/documents/10199/4547797/Effective+Until+August+29%2C+2008.xls
# https://www.msci.com/documents/10199/4547797/Effective+Until+June+30%2C+2010.xls
# https://www.msci.com/documents/10199/4547797/Effective+Until+February+28%2C+2014.xls
# https://www.msci.com/documents/10199/4547797/Effective+Until+August+31%2C+2016.xls
# https://www.msci.com/documents/1296102/11185224/Effective+until+September+28%2C+2018.xls

get_gics_url <- function(untilyear, untilmonth, untilday) {
  if (untilyear==2002) { 
    url <- "https://www.msci.com/documents/10199/4547797/Effective+From+1999+Until+March+28%2C+2002.xls"
  } else if (untilyear==2018) {
    url <- "https://www.msci.com/documents/1296102/11185224/Effective+until+September+28%2C+2018.xls"
  } else {
    url <- paste0("https://www.msci.com/documents/10199/4547797/Effective","+Until+",untilmonth,"+",untilday,"%2C+", untilyear,".xls")
  }
  url
}

get_GICS_url(2002,'March',28)
get_GICS_url(2003,'April',30)
get_GICS_url(2018,'September',28)

# Task 2. Get a GICS table
# Using "https://www.msci.com/documents/10199/4547797/Effective+From+1999+Until+March+28%2C+2002.xls" 
# as an example to extract a GICS table.

t <- tempfile(fileext = ".zip") 

url <- "https://www.msci.com/documents/10199/4547797/Effective+From+1999+Until+March+28%2C+2002.xls"

download.file(url, t)

gics_data <- 
  readxl::read_excel(t, 
                     col_names = c("eonomic_sector_code",	"eonomic_sector_desc",	
                                   "industry_group_code", "industry_group_desc",
                                   "industry_code",	"industry_desc",
                                   "subindustry_code",	"subindustry_desc"),
                     col_types = c("numeric", "text","numeric","text","numeric","text","numeric","text"),
                     na = "",
                     trim_ws = TRUE,
                     skip = 4) %>%
  tidyr::fill(eonomic_sector_code, eonomic_sector_desc,	
              industry_group_code, industry_group_desc,
              industry_code, industry_desc, subindustry_code,)
unlink(t)

gics_data

# Task 3. Export gics_data into a csv, and make a table that could be used for SQL merges

# Export gics_data into a csv using readr::write_csv
write_csv(gics_data, paste0("gics_data_", 2002, ".csv"), append = FALSE)

# Make a table that could be used for SQL merges
  pg <- dbConnect(RPostgres::Postgres())
  
  dbExecute(pg, "SET search_path TO ff")
  
  gics_data %>% 
    dbWriteTable(pg, paste0("gics_data_", 2002), .,
                 overwrite=TRUE, row.names=FALSE)
  
  rs <- dbExecute(pg, paste0("VACUUM gics_data_", 2002))
  rs <- dbExecute(pg, paste0("CREATE INDEX ON gics_data_", 2002,
                             " (eonomic_sector_code)"))
  
  dbExecute(pg, paste0("ALTER TABLE gics_data_", 2002, " OWNER TO ff"))
  dbExecute(pg, paste0("GRANT SELECT ON gics_data_", 2002, " TO ff_access"))
  
  sql <- paste0("
    COMMENT ON TABLE gics_data_", 2002, " IS
    'CREATED USING get_gics_ind.R ON ", Sys.time() , "';")
  rs <- dbExecute(pg, paste(sql, collapse="\n"))
  rs <- dbDisconnect(pg)

# Task 4. Combine Task 1, 2, and 3 into a function, which takes the Effective Until date and 
# outputs a corresponding GICS csv and a PG SQL table ready to merge.

get_gics_table <- function(untilyear, untilmonth, untilday) {
  
  if (untilyear==2002) { 
    url <- "https://www.msci.com/documents/10199/4547797/Effective+From+1999+Until+March+28%2C+2002.xls"
  } else if (untilyear==2018) {
    url <- "https://www.msci.com/documents/1296102/11185224/Effective+until+September+28%2C+2018.xls"
  } else {
    url <- paste0("https://www.msci.com/documents/10199/4547797/Effective","+Until+",untilmonth,"+",untilday,"%2C+", untilyear,".xls")
  }

  t <- tempfile(fileext = ".zip") 
  
  download.file(url, t)
  
  gics_data <- 
    readxl::read_excel(t, 
                       col_names = c("eonomic_sector_code",	"eonomic_sector_desc",	
                                     "industry_group_code", "industry_group_desc",
                                     "industry_code",	"industry_desc",
                                     "subindustry_code",	"subindustry_desc"),
                       col_types = c("numeric", "text","numeric","text","numeric","text","numeric","text"),
                       na = "",
                       trim_ws = TRUE,
                       skip = 4) %>%
    tidyr::fill(eonomic_sector_code, eonomic_sector_desc,	
                industry_group_code, industry_group_desc,
                industry_code, industry_desc, subindustry_code,)
  unlink(t)

  write_csv(gics_data, paste0("gics_data_", untilyear, ".csv"), append = FALSE)
  
  pg <- dbConnect(RPostgres::Postgres())
  
  dbExecute(pg, "SET search_path TO ff")
  
  gics_data %>% 
    dbWriteTable(pg, paste0("gics_data_", untilyear), .,
                 overwrite=TRUE, row.names=FALSE)
  
  rs <- dbExecute(pg, paste0("VACUUM gics_data_", untilyear))
  rs <- dbExecute(pg, paste0("CREATE INDEX ON gics_data_", untilyear,
                             " (eonomic_sector_code)"))
  
  dbExecute(pg, paste0("ALTER TABLE gics_data_", untilyear, " OWNER TO ff"))
  dbExecute(pg, paste0("GRANT SELECT ON gics_data_", untilyear, " TO ff_access"))
  
  sql <- paste0("
    COMMENT ON TABLE gics_data_", untilyear, " IS
    'CREATED USING get_gics_ind.R ON ", Sys.time() , "';")
  rs <- dbExecute(pg, paste(sql, collapse="\n"))
  rs <- dbDisconnect(pg)
}

get_gics_table(2002,'March',28)
get_gics_table(2003,'April',30)
get_gics_table(2018,'September',28)
```
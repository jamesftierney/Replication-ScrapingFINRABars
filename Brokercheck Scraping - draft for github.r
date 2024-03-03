library(tidyverse)
library(rvest)
library(lubridate)
library(jsonlite)
library(polite)

# -------------------------------------------------------------------------------------
# scrape the list of bars from FINRA's website

# begin by creating a polite scraping session
url <- "https://www.finra.org/rules-guidance/oversight-Oversight%20%26%20Enforcement/individuals-barred-finra"

session <- bow(url) 

bars_list <- session %>%
  scrape()

# extract information from an HTML table

table_data <- bars_list %>%
  html_nodes("table") 

# pull out the links from the table, where available, and add them as a new variable 

list_of_bars <- table_data %>%
  html_table() %>%
  .[[1]] %>%
  filter(!is.na(CRD)) %>%
  mutate(links = table_data %>%
           html_nodes('tr') %>%
           html_nodes('a') %>%
           html_attr("href"))

# define logic variable `crd` for whether the link is to a brokercheck record

scrapable_list_of_bars <- list_of_bars %>%
  mutate(crd = case_when(str_detect(links, "brokercheck.finra.org") ~ TRUE,
                         TRUE ~ FALSE)) 

# subset and save a list of scrapable bars with brokercheck records

scrapable_list_of_bars <- scrapable_list_of_bars %>%
  group_by(CRD) %>%
  unique() %>%
  tibble::rowid_to_column("index") 

saveRDS(scrapable_list_of_bars, "list_of_bars_feb_28_2024.RDS")

# subset the non-scrapable bars without brokercheck records, for manual review

manual_bars <- scrapable_list_of_bars %>% 
  ungroup() %>%
  filter(crd == FALSE) %>%
  arrange(links) 

saveRDS(manual_bars, "manual_bars.RDS")

# -------------------------------------------------------------------------------------
# scrape the list of bars from FINRA's website

# define a vector with the brokercheck CRD ids that we're goign to pull from brokecheck's api

vector_of_CRDs_to_scrape <- scrapable_list_of_bars %>%
  filter(crd == TRUE) %>% 
  .$CRD

saveRDS(vector_of_CRDs_to_scrape, "vector_of_CRDs_to_scrape.RDS")

# introduce myself, open up a new polite scraping session, and query the API as defined in `params3` 

user_agent = 'James Tierney law professor jtierney1@kentlaw.iit.edu; polite R package bot'

session <- "https://api.brokercheck.finra.org/" %>%
  bow()

params3 = list(
  `hl` = 'true',
  `includePrevious` = 'true',
  `nrows` = '12',
  # apparently there has to be a query value but it doesn't affect calling a vector of CRDs directly
  `query` = 'john',
  `r` = '25',
  `sort` = I('bc_lastname_sort+asc,bc_firstname_sort+asc,bc_middlename_sort+asc,score+desc'),
  `wt` = 'json'
)

bc_working_tibble <- NA 

# the following is the scraper function

extract_data <- function(url){
	# we're going to do this slowly and patiently, hard coded with a 1 second delay between calls to the API
  Sys.sleep(2)
  
  url_scraped <- nod(session, paste0("https://api.brokercheck.finra.org/search/individual/", url)) %>%
  scrape(query = params3)
  
  if (url_scraped$hits$total > 0) {
    json <- gsub("^angular\\.callbacks._1\\((.*)\\);?$", "\\1", url_scraped$hits$hits[[1]]$`_source`) %>%
      fromJSON() 
    
    bc_working_tibble <- tibble(
      individualId = json$basicInformation$individualId,
      firstName = json$basicInformation$firstName,
      middleName = json$basicInformation$middleName,
      lastName = json$basicInformation$lastName,
      otherNames = list(json$basicInformation$otherNames),
      sanctions = list(json$basicInformation$sanctions),
      bcScope = json$basicInformation$bcScope,
      iaScope = json$basicInformation$iaScope,
      daysInIndustry = json$basicInformation$daysInIndustry,
      currentEmployments = list(json$currentEmployments),
      currentIAEmployments = list(json$currentIAEmployments),
      previousEmployments = list(json$previousEmployments),
      previousIAEmployments = list(json$previousIAEmployments),
      disclosureFlag = list(json$disclosureFlag),
      iaDisclosureFlag = list(json$iaDisclosureFlag),
      disclosures = list(json$disclosures),
      examsCount = list(json$examsCount),
      stateExamCategory = list(json$stateExamCategory),
      principalExamCategory = list(json$principalExamCategory),
      productExamCategory = list(json$productExamCategory),
      registrationCount = list(json$registrationCount),
      registeredStates = list(json$registeredStates),
      registeredSROs = list(json$registeredSROs),
      brokerDetails = list(json$brokerDetails),
      match = TRUE
    )
    
  } else {
    
    bc_working_tibble <- tibble(
      match = FALSE
    )
  }
  
  return(bc_working_tibble)
  
}

map_dfr(vector_of_CRDs_to_scrape, ~extract_data(.))

# because i'm doing this on a laptop and i'm not smart enough to figure out how to
# get this to keep going without throwing an error, i iterate through short ranges
# of the data to make sure it is all going to work!

# celebrate your failures!

test_first_20 <- map_dfr(vector_of_CRDs_to_scrape[1:20], ~extract_data(.))

test_next_80 <- map_dfr(vector_of_CRDs_to_scrape[21:100], ~extract_data(.))

test_through_150 <- map_dfr(vector_of_CRDs_to_scrape[101:150], ~extract_data(.))

test_through_200 <- map_dfr(vector_of_CRDs_to_scrape[151:200], ~extract_data(.))

test_through_1000 <- map_dfr(vector_of_CRDs_to_scrape[201:1000], ~extract_data(.))

test_through_1000 <- rbind(test_first_20, test_next_80, test_through_150, test_through_200, test_through_1000)

test_through_1000

test_through_2000 <- map_dfr(vector_of_CRDs_to_scrape[1001:2000], ~extract_data(.))

test_through_3000 <- map_dfr(vector_of_CRDs_to_scrape[2001:3000], ~extract_data(.))

test_through_3100 <- map_dfr(vector_of_CRDs_to_scrape[3001:3100], ~extract_data(.))

test_through_4000 <- map_dfr(vector_of_CRDs_to_scrape[3101:4000], ~extract_data(.))

# cleanup, I think this doesn't throw any errors
test_through_4000 <- test_through_4000 %>%
  filter(!individualId %in% test_through_1000$individualId)

test_through_4000_total <- rbind(test_through_1000, test_through_2000, test_through_3000, test_through_3100, test_through_4000) %>%
  filter(!is.na(individualId)) %>%
  group_by(individualId) %>%
  unique() 
  
test_through_4000_total 

# find the particular ones that didn't parse
vector_of_CRDs_to_scrape[1:4000] %>%
  .[!(vector_of_CRDs_to_scrape %in% test_through_4000_total$individualId)] %>%
  .[1:3]
  
# these disappeared!
#test_through_4000_fill_in <- map_dfr(c(4787306, 1401945, 7262411) , ~extract_data(.))

test_through_4250 <- map_dfr(vector_of_CRDs_to_scrape[4001:4250], ~extract_data(.))

test_through_4500 <- map_dfr(vector_of_CRDs_to_scrape[4251:4500], ~extract_data(.))

test_through_4750 <- map_dfr(vector_of_CRDs_to_scrape[4501:4750], ~extract_data(.))

test_through_5000 <- map_dfr(vector_of_CRDs_to_scrape[4751:5000], ~extract_data(.))

test_through_5250 <- map_dfr(vector_of_CRDs_to_scrape[5001:5250], ~extract_data(.))

test_through_5500 <- map_dfr(vector_of_CRDs_to_scrape[5251:5500], ~extract_data(.))

test_through_6000 <- map_dfr(vector_of_CRDs_to_scrape[5501:6000], ~extract_data(.))

test_through_end <- map_dfr(vector_of_CRDs_to_scrape[6001:length(vector_of_CRDs_to_scrape)], ~extract_data(.))

test_through_end_total <- rbind(test_through_4000_total, 
      test_through_4250, 
      test_through_4500, 
      test_through_4750,
      test_through_5000,
      test_through_5250,
      test_through_5500,
      test_through_6000,
      test_through_end)
      
test_through_end_total %>%
  filter(is.na(individualId)) 
  select(individualId, disclosures) %>%
  group_by(individualId) %>%
  filter(n() > 1) 

test_list_of_bars <- scrapable_list_of_bars %>%
  ungroup() %>%
  # we have at least one duplicate name on the scraping list, so we drop the second instance
  filter(!(index == 8896)) %>%
  left_join(test_through_end_total, 
            by = c("CRD" = "individualId")) %>%
  unnest_wider(sanctions) %>%
  unnest_wider(disclosures) %>%
  unnest(cols = c(eventDate, disclosureType, disclosureResolution, disclosureDetail, isIapdExcludedCCFlag, isBcExcludedCCFlag, bcCtgryType, iaCtgryType)) 

test_list_of_bars <- test_list_of_bars %>%
  mutate(SanctionDetails = map(SanctionDetails, function(x) {
    if (is.list(x) && is_null(x) == TRUE) {
      x <- data.frame(matrix(ncol = 0, nrow = 0))
    }
    if (is.list(x) && length(x) == 0) {
      x <- data.frame(matrix(ncol = 0, nrow = 0))
    }
    return(x)
  })) 

test_list_of_bars_unnested <- test_list_of_bars %>%
  unnest(SanctionDetails, names_repair = "unique") 

saveRDS(test_list_of_bars, "test_list_of_bars_03_01_2024.RDS")
saveRDS(test_list_of_bars_unnested, "test_list_of_bars_unnested_03_01_2024.RDS")



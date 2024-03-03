library(tidyverse)
library(rvest)
library(lubridate)
library(jsonlite)
library(stringi)
library(skimr)


# ---- original analysis and replication on nov 13 from below

# list of bars scraped from the finra bars website
list_of_bars <- readRDS("list_of_bars.RDS")

# need to drop rows with duplicate entries
# so create a vector of duped indexes to drop
#dropped_bars <- test_list_of_bars %>%
#  group_by(CRD) %>%
#  filter(n() > 1) %>%
#  ungroup() %>%
#  # each duplicate entry has only two, so we can select just the second entry
#  slice(which(row_number() %% 2 == 1)) %>%
#  .$index 

test_list_of_bars_unnested 

#test_list_of_bars <- test_list_of_bars_unnested %>%
#  filter(!(index %in% dropped_bars))

# stuff i scraped from brokercheck -- will upload that and this script, when cleaned, to github
#scraped_from_brokercheck <- readRDS("scraped_from_brokercheck_2023_11_13.RDS")

# stuff my RAs and I hand-coded
list_of_bars_manual <- read_csv("list_of_bars_manual (1).csv")

initiated_by_unclean <-  
  test_list_of_bars_unnested %>%
  full_join(list_of_bars_manual) %>%
  rename(has_crd = crd) %>%
  mutate(off_jurisdiction = case_when(has_crd == TRUE & is.na(bcCtgryType) ~ TRUE,
                                      TRUE ~ FALSE)) %>%
#  filter(str_detect(`Sanctions...14`, 'Bar')) %>%
  mutate(`Initiated By` = tolower(`Initiated By`)) %>%
  mutate(barring_agency = case_when(str_detect(disclosure, "FINRA|NASD") ~ "finra",
                                    str_detect(categorical, "FINRA|NASD") ~ "finra",
                                    str_detect(`Initiated By`, "alabama|\\bal\\b") ~ "alabama",
                                    str_detect(`Initiated By`, "alaska") ~ "alaska",
                                    str_detect(`Initiated By`, "arizona") ~ "arizona",
                                    str_detect(`Initiated By`, "arkansas") ~ "arkansas",
                                    str_detect(`Initiated By`, "california|business oversight|ca-sf|ca depart") ~ "california",
                                    str_detect(`Initiated By`, "colorado") ~ "colorado",
                                    str_detect(`Initiated By`, "connecticut") ~ "connecticut",
                                    str_detect(`Initiated By`, "district of  columbia") ~ "district of columbia",
                                    str_detect(`Initiated By`, "delaware") ~ "delaware",
                                    str_detect(`Initiated By`, "florida") ~ "florida",
                                    str_detect(`Initiated By`, "georgia|ga sec") ~ "georgia",
                                    str_detect(`Initiated By`, "hawaii") ~ "hawaii",
                                    str_detect(`Initiated By`, "idaho") ~ "idaho",
                                    str_detect(`Initiated By`, "illinois") ~ "illinois",
                                    str_detect(`Initiated By`, "indiana|\\bin\\b") ~ "indiana",
                                    str_detect(`Initiated By`, "iowa") ~ "iowa",
                                    str_detect(`Initiated By`, "kansas") ~ "kansas",
                                    str_detect(`Initiated By`, "kentucky") ~ "kentucky",
                                    str_detect(`Initiated By`, "louisiana") ~ "louisiana",
                                    str_detect(`Initiated By`, "maine") ~ "maine",
                                    str_detect(`Initiated By`, "maryland") ~ "maryland",
                                    str_detect(`Initiated By`, "massachusetts|commonwealth of ma") ~ "massachusetts",
                                    str_detect(`Initiated By`, "michigan|csld") ~ "michigan",
                                    str_detect(`Initiated By`, "minnesota|mn\\b") ~ "minnesota",
                                    str_detect(`Initiated By`, "mississippi") ~ "mississippi",
                                    str_detect(`Initiated By`, "missouri") ~ "missouri",
                                    str_detect(`Initiated By`, "montana") ~ "montana",
                                    str_detect(`Initiated By`, "nebraska") ~ "nebraska",
                                    str_detect(`Initiated By`, "nevada") ~ "nevada",
                                    str_detect(`Initiated By`, "new hampshire|nh\\b") ~ "new hampshire",
                                    str_detect(`Initiated By`, "new jersey") ~ "new jersey",
                                    str_detect(`Initiated By`, "new mexico") ~ "new mexico",
                                    str_detect(`Initiated By`, "new york") ~ "new york",
                                    str_detect(`Initiated By`, "north carolina") ~ "north carolina",
                                    str_detect(`Initiated By`, "north dakota") ~ "north dakota",
                                    str_detect(`Initiated By`, "ohio") ~ "ohio",
                                    str_detect(`Initiated By`, "oklahoma") ~ "oklahoma",
                                    str_detect(`Initiated By`, "oregon") ~ "oregon",
                                    str_detect(`Initiated By`, "pennsylvania|pa\\b") ~ "pennsylvania",
                                    str_detect(`Initiated By`, "rhode island") ~ "rhode island",
                                    str_detect(`Initiated By`, "tennessee|tn department") ~ "tennessee",
                                    str_detect(`Initiated By`, "texas|tx state") ~ "texas",
                                    str_detect(`Initiated By`, "south carolina|sc securities") ~ "south carolina",
                                    str_detect(`Initiated By`, "south dakota") ~ "south dakota",
                                    str_detect(`Initiated By`, "utah") ~ "utah",
                                    str_detect(`Initiated By`, "vermont") ~ "vermont",
                                    str_detect(`Initiated By`, "west virginia") ~ "west virginia",
                                    str_detect(`Initiated By`, "washington|wa dept|olympia") ~ "washington",
                                    str_detect(`Initiated By`, "virginia") ~ "virginia",
                                    str_detect(`Initiated By`, "wisconsin") ~ "wisconsin",
                                    str_detect(`Initiated By`, "coffee") ~ "coffee, sugar, cocoa",
                                    str_detect(`Initiated By`, "commodity futures") ~ "cftc",
                                    str_detect(`Initiated By`, "chicago board") ~ "cboe",
                                    str_detect(`Initiated By`, "nasdaq") ~ "nasdaq",
                                    str_detect(`Initiated By`, "nyse arca") ~ "nyse arca",
                                    str_detect(`Initiated By`, "nyse mkt") ~ "nyse mkt",
                                    str_detect(`Initiated By`, "nyse national") ~ "nyse national",
                                    str_detect(`Initiated By`, "nyse") ~ "nyse",
                                    str_detect(`Initiated By`, "national futures") ~ "nfa",
                                    str_detect(`Initiated By`, "puerto rico") ~ "puerto rico",
                                    str_detect(`Initiated By`, "comptroller|department of labor|thrift|federal deposit") ~ "other federal",
                                    str_detect(`Initiated By`, "securities and exchange|\\bsec\\b") ~ "sec",
                                    str_detect(`Initiated By`, "finra|firna|financial industry regulatory authority|nasd|national assoc|hearing panel|failure to respond") ~ "finra",
                                    str_detect(`Initiated By`, "other|not provided|blalack") ~ "other",
                                    str_detect(`Initiated By`, "bats") ~ "bats exchange",
                                    str_detect(`Initiated By`, "chx\\b") ~ "chx",
                                    str_detect(`Initiated By`, "american stock exchange") ~ "ase",
                                    str_detect(`Initiated By`, "international secur") ~ "ise",
                                    str_detect(`Initiated By`, "byx|bzx|edga|edgx|cboe exchange|cboe") ~ "cboe exchange",
                                    str_detect(`Initiated By`, "singapore") ~ "foreign regulators",
                                    str_detect(`Initiated By`, "canada") ~ "foreign regulators",
                                    str_detect(`Initiated By`, "midwest stock") ~ "midwest stock exchange",
                                    str_detect(`Initiated By`, "philadelphia stock") ~ "stock exchange",
                                    str_detect(`Initiated By`, "duplicate record|first albany") ~ "bats exchange", 
                                    TRUE ~ NA)) %>%
  mutate(barring_agency_category = case_when(barring_agency == "alabama" ~ "state",
                                    barring_agency == "alaska" ~ "state",
                                    barring_agency ==  "arizona" ~ "state",
                                    barring_agency ==  "arkansas" ~ "state",
                                    barring_agency ==  "california" ~ "state",
                                    barring_agency ==  "colorado" ~ "state",
                                    barring_agency ==  "connecticut" ~ "state",
                                    barring_agency ==  "district of columbia" ~ "state",
                                    barring_agency ==  "delaware" ~ "state",
                                    barring_agency ==  "florida" ~ "state",
                                    barring_agency ==  "georgia" ~ "state",
                                    barring_agency ==  "hawaii" ~ "state",
                                    barring_agency ==  "idaho" ~ "state",
                                    barring_agency ==  "illinois" ~ "state",
                                    barring_agency ==  "indiana" ~ "state",
                                    barring_agency ==  "iowa" ~ "state",
                                    barring_agency ==  "kansas" ~ "state",
                                    barring_agency ==  "kentucky" ~ "state",
                                    barring_agency ==  "louisiana" ~ "state",
                                    barring_agency ==  "maine" ~ "state",
                                    barring_agency ==  "maryland" ~ "state",
                                    barring_agency ==  "massachusetts" ~ "state",
                                    barring_agency ==  "michigan" ~ "state",
                                    barring_agency ==  "minnesota" ~ "state",
                                    barring_agency ==  "mississippi" ~ "state",
                                    barring_agency ==  "missouri" ~ "state",
                                    barring_agency ==  "montana" ~ "state",
                                    barring_agency ==  "nebraska" ~ "state",
                                    barring_agency ==  "nevada" ~ "state",
                                    barring_agency ==  "new hampshire" ~ "state",
                                    barring_agency ==  "new jersey" ~ "state",
                                    barring_agency ==  "new mexico" ~ "state",
                                    barring_agency ==  "new york" ~ "state",
                                    barring_agency ==  "north carolina" ~ "state",
                                    barring_agency ==  "north dakota" ~ "state",
                                    barring_agency ==  "ohio" ~ "state",
                                    barring_agency ==  "oklahoma" ~ "state",
                                    barring_agency ==  "oregon" ~ "state",
                                    barring_agency ==  "pennsylvania" ~ "state",
                                    barring_agency ==  "rhode island" ~ "state",
                                    barring_agency ==  "tennessee" ~ "state",
                                    barring_agency ==  "texas" ~ "state",
                                    barring_agency ==  "south carolina" ~ "state",
                                    barring_agency ==  "south dakota" ~ "state",
                                    barring_agency ==  "utah" ~ "state",
                                    barring_agency ==  "vermont" ~ "state",
                                    barring_agency ==  "west virginia" ~ "state",
                                    barring_agency ==  "washington" ~ "state",
                                    barring_agency ==  "virginia" ~ "state",
                                    barring_agency ==  "wisconsin" ~ "state",
                                    barring_agency ==  "coffee, sugar, cocoa" ~ "commodities or options exchange",
                                    barring_agency ==  "cftc" ~ "cftc",
                                    barring_agency ==  "cboe" ~ "commodities or options exchange",
                                    barring_agency ==  "nasdaq" ~ "stock exchange",
                                    barring_agency ==  "nyse arca" ~ "stock exchange",
                                    barring_agency ==  "nyse mkt" ~ "stock exchange",
                                    barring_agency ==  "nyse national" ~ "stock exchange",
                                    barring_agency ==  "nyse" ~ "stock exchange",
                                    barring_agency ==  "nfa" ~ "nfa",
                                    barring_agency ==  "puerto rico" ~ "state",
                                    barring_agency ==  "other federal" ~ "other",
                                    barring_agency ==  "sec" ~ "sec",
                                    barring_agency ==  "finra" ~ "finra",
                                    barring_agency ==  "other" ~ "other",
                                    barring_agency ==  "bats exchange|chx" ~ "stock exchange",
                                    barring_agency ==  "ise" ~ "commodities or options exchange",
                                    barring_agency ==  "cboe exchange|midwest stock|philadelphia stock" ~ "stock exchange",
                                    barring_agency ==  "foreign regulators" ~ "other",
                                    TRUE ~ NA))

# there are a lot of other folks who have imposed sanctions in the brokercheck data
# but we are mainly interested in finra here, so let's subset it to the bars where we do and don't have data

# there is a many to many relationship [7516,] of x and [7404,] of y

initiated_by_unclean_backup <- initiated_by_unclean

# this got messed up in the last analysis

finra_hand_coded_disclosures <- initiated_by_unclean %>%
  filter(barring_agency == "finra",
         is.na(Sanctions...32)) %>%
  mutate(category = "Textual or categorical (BrokerCheck record unavailable)")

finra_just_bars <- initiated_by_unclean %>%
  filter(barring_agency == "finra",
         str_detect(Sanctions...32, "Bar"))  %>%
  mutate(category = "FINRA BrokerCheck record available")

finra_other_sanctions <- initiated_by_unclean %>%
  filter(barring_agency == "finra",
         !str_detect(Sanctions...32, "Bar"))   %>%
  mutate(category = "Other sanctions imposed by FINRA in BrokerCheck records")

# HAVE TO FIX THIS WITH THE DUPES
total_count <- rbind(
  finra_hand_coded_disclosures,
  finra_just_bars
)



# ------------
# we're going to make a new list of all the categoricals
categoricals_temp <- total_count %>%
  filter(!(categorical) == "") %>%
  select(index, CRD, categorical) 

# the categoricals tend to have a date field in a second set of parentheses in a string.
# so rather than playing with regex to get the second match to a within-parentheses pattern,
# we're going to extract all the within-parentheses strings, and drop all those that don't
# parse to a date
categoricals_in_transit <- categoricals_temp %>%
  mutate(extracted = str_extract_all(categorical, "\\(([^)]+)\\)")) %>%
  select(-categorical) %>%
  unnest(extracted) %>%
  mutate(extracted = str_sub(extracted, 2, -2)) %>%
  mutate(extracted = mdy(extracted)) %>%
  filter(!is.na(extracted))

categoricals_temp <- categoricals_temp %>%
  left_join(categoricals_in_transit)

categoricals_custom <- categoricals_temp %>%
  filter(is.na(extracted)) 

# same number of unique indexes before and after

# Assign missing four values

categoricals_custom$extracted <- c(
  # imputed from https://www.finra.org/sites/default/files/RCA/p002371.pdf
  # did not parse because of parentheses
  ymd("2014-07-14"), 
  ymd("2002-11-01"),
  ymd("2015-09-04"), 
  ymd("2002-12-02")
)

categoricals_temp <- categoricals_temp %>%
  full_join(categoricals_custom) %>%
  filter(!is.na(extracted)) 

total_count_with_dates <- total_count %>%
  full_join(categoricals_temp) %>%
  arrange(index) %>%
  mutate(eventDate = as.character(mdy(eventDate)), 
         eventDate = ifelse(is.na(eventDate), ifelse(is.na(extracted), NA, as.character(extracted)), as.character(eventDate)),
         eventDate = as.Date(eventDate)
  ) %>%
  select(-extracted)

# MUST DEAL WITH THESE BEFORE PRODUCTION

dupes <- total_count_with_dates %>%
  arrange(index) %>%
  group_by(CRD) %>%
  filter(n() > 1) %>%
  #  select(index, , CRD, `Initiated By`, Allegations, Resolution, Sanctions...32, Sanctions...38, disclosure, categorical) %>%
  select(index, CRD) %>%
  unique() %>%
  ungroup() #%>%
#write_csv("dupes.csv")

finra_dupes <- total_count_with_dates %>%
  filter(CRD %in% dupes$CRD)

first_finra_dupes <- finra_dupes %>%
  select(index, CRD, eventDate, Allegations, `Regulator Statement`) %>%
  group_by(index) %>%
  arrange(eventDate, .by_group = TRUE) %>%
  slice(1L) %>%
  mutate(sequence = fct("First Bar"))

second_finra_dupes <- finra_dupes %>%
  select(index, CRD, eventDate, Allegations, `Regulator Statement`) %>%
  group_by(index) %>%
  arrange(eventDate, .by_group = TRUE) %>%
  mutate(new_row = row_number(),
         max_row = max(new_row)) %>%
  slice(2:n()) %>%
  mutate(sequence = fct("Subsequent Bar"))

middle_drops <- second_finra_dupes %>%
  ungroup() %>%
  group_by(CRD) %>%
  filter(!new_row == max_row) %>%
  rename(`Number of Bars` = max_row) %>%
  ungroup() 

first_bars <- first_finra_dupes %>%
  ungroup() %>%
  select(-sequence)

last_bars <- second_finra_dupes %>%
  ungroup() %>%
  group_by(CRD) %>%
  filter(new_row == max_row) %>%
  rename(`Number of Bars` = max_row,
         `Second Bar` = eventDate) %>%
  ungroup() 
  
finra_dupe_drops <- last_bars %>%
  rename(eventDate = `Second Bar`) %>%
  full_join(middle_drops) %>%
  select(-c(sequence, `Number of Bars`))

multiple_bars <- first_bars %>%
  rename(`First Bar` = eventDate) %>%
  full_join(last_bars, by = c("index", "CRD")) %>%
  mutate(`Days between Bars` = `Second Bar` - `First Bar`) %>%
  select(-c(sequence, Allegations.x, Allegations.y, `Regulator Statement.x`, `Regulator Statement.y`, `Number of Bars`))

ft_summarize_count <- total_count_with_dates %>%
  group_by(index) %>%
  arrange(eventDate, .by_group = TRUE) %>%
  mutate(new_row = row_number(),
         max_row = max(new_row)) %>%
  anti_join(second_finra_dupes, by = join_by(index, CRD, eventDate, new_row)) %>%
  mutate(year = case_when(!is.na(eventDate) ~ year(eventDate),
                          TRUE ~ NA)) %>%
  filter(!is.na(year)) %>%
  group_by(year) %>%
  summarize(count = n()) %>%
  filter(year > 1998) %>%
  #mutate(`Imputed count` = round(count + (372)/23)) %>%
  rename(Year = year,
         Count = count) 

ft_summarize_flextable <- ft_summarize_count %>%
  flextable() 

ft_summarize_flextable <- colformat_double(
  x = ft_summarize_flextable,
  big.mark = "", digits = 0
)

ft_summarize_flextable %>%
  autofit() %>% 
  #  width(j=~x,width=1) %>% width(j=~y,width=1) %>%
  save_as_docx(path = "Table 3 --- annual_counts.docx")

ft_summarize_count %>%
  #rename(`Naively imputed count` = `Imputed count`) %>%
  pivot_longer(!Year, names_to = "Type", values_to = "Count") %>%
  mutate(Type = as.factor(Type)) %>%
  ggplot(mapping = aes(x = Year,
                       y = Count,
                       linetype = Type)) +
  geom_line() +
  geom_line(colour = 'grey70') +
  labs(title = "Annual counts of FINRA bars") +
  xlab("Year") +
  ylab("Number of bars in category") +
  theme_minimal() 

ggsave("Figure 1 --- FINRA_annual_counts.png",
       dpi = 300,
       scale = 0.5)



#install.packages("flextable")
library(flextable)

summarize_count <- total_count_with_dates %>%
  group_by(index) %>%
  arrange(eventDate, .by_group = TRUE) %>%
  mutate(new_row = row_number(),
         max_row = max(new_row)) %>%
  anti_join(second_finra_dupes, by = join_by(index, CRD, eventDate, new_row)) %>%
  select(index, CRD, `Sanctions...32`, bcCtgryType, off_jurisdiction, disclosure, categorical, eventDate, Resolution, `Sanction Details`, `Regulator Statement`, Allegations, category, new_row, max_row) %>%
  mutate(type = case_when(!(disclosure == "") ~ "Textual disclosure available",
                          !(categorical == "") ~ "Categorical disclosures (dates available)",
                          !is.na(eventDate) ~ "FINRA BrokerCheck record available --- first bars",
                          TRUE ~ "Not scrapable (only name and CRD available)"),
         type = as_factor(type),
         year = case_when(!is.na(eventDate) ~ year(eventDate),
                          TRUE ~ NA))

summarize_count %>%
  group_by(type) %>%
  summarize(n()) %>%
  ungroup() %>%
  mutate(type = as.character(type)) %>%
  rbind(c(type = "FINRA BrokerCheck record available --- subsequent bars", `n()` = 76),
    c(type = "Total", `n()` = 284 + 372 + 8176 + 76)) %>%
  rename(`Type of data` = type) %>%
  rename(Count = `n()`) %>%
  regulartable() %>%
  autofit() %>% 
  #  width(j=~x,width=1) %>% width(j=~y,width=1) %>%
  save_as_docx(path = "Table 1.docx")


#####################################################
# now let's find out the substance of the bars

# begin by defining some search terms

rule8210_search_string <- "8210|to respond|provide|testimony|requests for|request for|failed to produce|failure to produce|refused to produce|refused to appear|failed to produce|failed to appear|on\\-the\\-record|interview|to update"

fraud_search_string <- "10b\\-5|10\\(b\\)|fals|fictitious|forg|fake|misrepr|fraud|antifraud|deceiv|decept"


string_for_2010 <- "2110|just and equit|rule 2010|rules 2010|finra 2010|rules 2110\\, 2010|rule 2010|rules 2010|finra 2010|rules 2110\\, 2010|rules 1122\\, 2010|2020 and 2010|2020\\, 2111 and 2010|8210\\, 2010|1122 and 2010|2010\\, 8210|1250\\, 2010|2020\\, and 2010|8210 \\& 2010|2150\\(a\\)\\, 2010"

expedited_9552_search_string <- "9552"

expedited_9553_search_string <- "fail to pay|fails to pay|failed to pay"

expedited_9554_search_string <- "9554"

total_count_for_scraping <- summarize_count %>%
  group_by(index) %>%
  arrange(eventDate, .by_group = TRUE) %>%
  mutate(new_row = row_number(),
         max_row = max(new_row)) %>%
  ungroup() %>%
  anti_join(second_finra_dupes, by = join_by(index, CRD, eventDate, new_row)) %>%
  mutate(Resolution = as.factor(Resolution)) %>%
  mutate(disclosure = tolower(stri_trans_general(disclosure, "Publishing-Any; Any-ASCII")),
         `Sanction Details` = tolower(stri_trans_general(`Sanction Details`, "Publishing-Any; Any-ASCII")),
         `Regulator Statement` = tolower(stri_trans_general(`Regulator Statement`, "Publishing-Any; Any-ASCII")),
         categorical = tolower(stri_trans_general(categorical, "Publishing-Any; Any-ASCII"))) %>%
  unite("text_findings", c(`Sanction Details`, `Regulator Statement`, disclosure, categorical), sep = "", na.rm = TRUE, remove = FALSE) #%>%
  #mutate(year = year(eventDate))

total_count_for_scraping <- total_count_for_scraping %>%
  mutate(Resolution = fct_na_value_to_level(Resolution, "Missing Data"),
         Resolution = fct_collapse(Resolution,
               Settlement = c("Acceptance, Waiver & Consent(AWC)", 
                              "Consent",
                              "Decision & Order of Offer of Settlement"),
               Letter = c("Letter",
                          "LETTER",
                          "letter",
                          "LETTTER",
                          "BARRED LETTER"),
               Adjudication = c("Decision",
                                "Order",
                                "Judgment"),
               `Affirmed on Appeal` = c("U.S. COURT OF APPEALS DECISION",
                                        "Opinion of the Commission",
                                        "Order dismissing application for review"),
               Pending = c("Pending appeal",
                         "Pending Appeal"))) 

total_count_for_scraping$Resolution %>%
  fct_count()

total_count_for_scraping <- total_count_for_scraping %>% 
  mutate(text_findings = stringr::str_conv(text_findings, "UTF-8"),
    Allegations = tolower(Allegations),
         text_findings = tolower(text_findings),
         rule8210_found = str_detect(text_findings, rule8210_search_string),
         rule8210_alleged = str_detect(Allegations, rule8210_search_string),
         fraud_alleged = str_detect(Allegations, fraud_search_string),
         fraud_found = str_detect(text_findings, fraud_search_string),
         expedited_9552 = case_when(str_detect(text_findings, expedited_9552_search_string) ~ TRUE,
                                    #str_detect(Allegations, expedited_9552_search_string) ~ TRUE,
                                    TRUE ~ FALSE),
         expedited_9553 = case_when(str_detect(text_findings, expedited_9553_search_string) ~ TRUE,
                                    str_detect(Allegations, expedited_9553_search_string) ~ TRUE,
                                    TRUE ~ FALSE),
         expedited_9554 = case_when(str_detect(text_findings, expedited_9554_search_string) ~ TRUE,
                                    str_detect(Allegations, expedited_9554_search_string) ~ TRUE,
                                    TRUE ~ FALSE),
         failtopay = expedited_9553,
         conversion_alleged = str_detect(Allegations, "convert|convers|misapprop|commingl"),
         conversion_found = str_detect(text_findings, "convert|convers|misapprop|commingl"),
         pst_alleged = str_detect(Allegations, "private securities|unregistered securities"),
         pst_found = str_detect(text_findings, "private securities|unregistered securities"),
         just_and_equitable_alleged = str_detect(Allegations, string_for_2010),
         just_and_equitable_found = str_detect(text_findings, string_for_2010),
         exam_alleged = str_detect(Allegations, "exam"),
         exam_found = str_detect(text_findings, "exam"),
         suitability_alleged = str_detect(Allegations, "suitab"),
         suitability_found = str_detect(text_findings, "suitab"),
         disqualification_alleged = str_detect(Allegations, "disqual"),
         disqualification_found = str_detect(text_findings, "disqual"),
         u4_alleged = str_detect(Allegations, "u4"),
         u4_found = str_detect(text_findings, "u4"),
         outsidebusiness_alleged = str_detect(Allegations, "outside business|transacted away"),
         outsidebusiness_found = str_detect(text_findings, "outside business|transacted away"),         
         withoutadmitting = case_when(str_detect(Allegations, "without admitting") ~ TRUE,
                                      str_detect(text_findings, "without admitting") ~ TRUE,
                                      str_detect(`Regulator Statement`, "without admitting") ~ TRUE,
                                      TRUE ~ FALSE),
         expedited = case_when(str_detect(Allegations, "expedit") ~ TRUE,
                                      str_detect(text_findings, "expedit") ~ TRUE,
                                      Resolution == "Letter" ~ TRUE,
                                      TRUE ~ FALSE),
         by_consent_detected = case_when(str_detect(Allegations, "consent") ~ TRUE,
                                         Resolution == "Settlement" ~ TRUE,
                                         str_detect(`Regulator Statement`, "consent") ~ TRUE,
                                         TRUE ~ FALSE)) 




rule_8210_alleged_time_series <- total_count_for_scraping %>%
#  filter(year > 1999,
#         year < 2023) %>%
    group_by(rule8210_alleged, year) %>%
  count() %>%
  pivot_wider(names_from = rule8210_alleged, values_from = n) %>%
    ungroup() %>%
  mutate(proportion_8210_alleged = `TRUE` / (`FALSE` + `TRUE`))

rule_8210_found_time_series <- total_count_for_scraping %>%
  #  filter(year > 1999,
  #         year < 2023) %>%
  group_by(rule8210_found, year) %>%
  count() %>%
  pivot_wider(names_from = rule8210_found, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_8210_found = `TRUE` / (`FALSE` + `TRUE`))

conversion_alleged_time_series <- total_count_for_scraping %>%
  group_by(conversion_alleged, year) %>%
  count() %>%
  pivot_wider(names_from = conversion_alleged, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_conversion_alleged = `TRUE` / (`FALSE` + `TRUE`))

conversion_found_time_series <- total_count_for_scraping %>%
  group_by(conversion_found, year) %>%
  count() %>%
  pivot_wider(names_from = conversion_found, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_conversion_found = `TRUE` / (`FALSE` + `TRUE`))

fraud_found_time_series <- total_count_for_scraping %>%
  group_by(fraud_found, year) %>%
  count() %>%
  pivot_wider(names_from = fraud_found, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_fraud_found = `TRUE` / (`FALSE` + `TRUE`))

fraud_alleged_time_series <- total_count_for_scraping %>%
  group_by(fraud_alleged, year) %>%
  count() %>%
  pivot_wider(names_from = fraud_alleged, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_fraud_alleged = `TRUE` / (`FALSE` + `TRUE`))

pst_alleged_time_series <- total_count_for_scraping %>%
  group_by(pst_alleged, year) %>%
  count() %>%
  pivot_wider(names_from = pst_alleged, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_pst_alleged = `TRUE` / (`FALSE` + `TRUE`))

pst_found_time_series <- total_count_for_scraping %>%
  group_by(pst_found, year) %>%
  count() %>%
  pivot_wider(names_from = pst_found, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_pst_found = `TRUE` / (`FALSE` + `TRUE`))

expedited_9552_time_series <- total_count_for_scraping %>%
  group_by(expedited_9552, year) %>%
  count() %>%
  pivot_wider(names_from = expedited_9552, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_expedited_9552 = `TRUE` / (`FALSE` + `TRUE`),
         expedited_9552 = `TRUE`)



expedited_9553_time_series <- total_count_for_scraping %>%
  group_by(expedited_9553, year) %>%
  count() %>%
  pivot_wider(names_from = expedited_9553, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_expedited_9553 = `TRUE` / (`FALSE` + `TRUE`))

expedited_9554_time_series <- total_count_for_scraping %>%
  group_by(expedited_9554, year) %>%
  count() %>%
  pivot_wider(names_from = expedited_9554, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_expedited_9554 = `TRUE` / (`FALSE` + `TRUE`))

exam_found_time_series <- total_count_for_scraping %>%
  group_by(exam_found, year) %>%
  count() %>%
  pivot_wider(names_from = exam_found, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_exam_found = `TRUE` / (`FALSE` + `TRUE`))

exam_alleged_time_series <- total_count_for_scraping %>%
  group_by(exam_alleged, year) %>%
  count() %>%
  pivot_wider(names_from = exam_alleged, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_exam_alleged = `TRUE` / (`FALSE` + `TRUE`))

just_and_equitable_alleged_time_series <- total_count_for_scraping %>%
  group_by(just_and_equitable_alleged, year) %>%
  count() %>%
  pivot_wider(names_from = just_and_equitable_alleged, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_just_and_equitable_alleged = `TRUE` / (`FALSE` + `TRUE`))

just_and_equitable_found_time_series <- total_count_for_scraping %>%
  group_by(just_and_equitable_found, year) %>%
  count() %>%
  pivot_wider(names_from = just_and_equitable_found, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_just_and_equitable_found = `TRUE` / (`FALSE` + `TRUE`))

suitability_alleged_time_series <- total_count_for_scraping %>%
  group_by(suitability_alleged, year) %>%
  count() %>%
  pivot_wider(names_from = suitability_alleged, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_suitability_alleged = `TRUE` / (`FALSE` + `TRUE`))

suitability_found_time_series <- total_count_for_scraping %>%
  group_by(suitability_found, year) %>%
  count() %>%
  pivot_wider(names_from = suitability_found, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_suitability_found = `TRUE` / (`FALSE` + `TRUE`))

u4_alleged_time_series <- total_count_for_scraping %>%
  group_by(u4_alleged, year) %>%
  count() %>%
  pivot_wider(names_from = u4_alleged, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_u4_alleged = `TRUE` / (`FALSE` + `TRUE`))

u4_found_time_series <- total_count_for_scraping %>%
  group_by(u4_found, year) %>%
  count() %>%
  pivot_wider(names_from = u4_found, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_u4_found = `TRUE` / (`FALSE` + `TRUE`))

disqualification_alleged_time_series <- total_count_for_scraping %>%
  group_by(disqualification_alleged, year) %>%
  count() %>%
  pivot_wider(names_from = disqualification_alleged, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_disqualification_alleged = `TRUE` / (`FALSE` + `TRUE`))

disqualification_found_time_series <- total_count_for_scraping %>%
  group_by(disqualification_found, year) %>%
  count() %>%
  pivot_wider(names_from = disqualification_found, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_disqualification_found = `TRUE` / (`FALSE` + `TRUE`))

outsidebusiness_alleged_time_series <- total_count_for_scraping %>%
  group_by(outsidebusiness_alleged, year) %>%
  count() %>%
  pivot_wider(names_from = outsidebusiness_alleged, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_outsidebusiness_alleged = `TRUE` / (`FALSE` + `TRUE`))

outsidebusiness_found_time_series <- total_count_for_scraping %>%
  group_by(outsidebusiness_found, year) %>%
  count() %>%
  pivot_wider(names_from = outsidebusiness_found, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_outsidebusiness_found = `TRUE` / (`FALSE` + `TRUE`))

expedited_time_series <- total_count_for_scraping %>%
  group_by(expedited, year) %>%
  count() %>%
  pivot_wider(names_from = expedited, values_from = n) %>%
  ungroup() %>%
  mutate(expedited = `TRUE` / (`FALSE` + `TRUE`))

not_accounted_for_time_series <- total_count_for_scraping %>%
  filter(rule8210_found == "FALSE",
         rule8210_alleged == "FALSE",
         fraud_alleged == "FALSE",
         fraud_found == "FALSE",
         expedited_9552 == "FALSE",
         expedited_9554 == "FALSE",
         conversion_alleged == "FALSE",
         conversion_found == "FALSE",
         pst_alleged == "FALSE",
         pst_found == "FALSE",
         just_and_equitable_alleged == "FALSE",
         just_and_equitable_found == "FALSE",
         exam_alleged == "FALSE",
         exam_found == "FALSE",
         suitability_alleged == "FALSE",
         suitability_found == "FALSE",
         disqualification_alleged == "FALSE",
         disqualification_found == "FALSE",
         u4_alleged == "FALSE",
         u4_found == "FALSE",
         outsidebusiness_alleged == "FALSE",
         outsidebusiness_found == "FALSE",
         expedited == "FALSE") %>%
  select(index, eventDate, Resolution, `Sanction Details`, `Regulator Statement`, Allegations, year)

write_csv(not_accounted_for_time_series, "not_accounted_for_time_series.csv")

not_accounted_for_time_series %>%
  group_by(year) %>%
  count() %>%
  rename(`not accounted for` = n)



total_count_summary <- ft_summarize_count %>%
  rename(year = Year) %>%
  left_join(rule_8210_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`, `NA`)) %>%
  left_join(rule_8210_found_time_series) %>%
  select(-c(`FALSE`, `TRUE`)) %>%
  left_join(conversion_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`, `NA`)) %>%
  left_join(conversion_found_time_series) %>%
  select(-c(`FALSE`, `TRUE`)) %>%
  left_join(fraud_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(fraud_found_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(expedited_9552_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(expedited_9553_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(expedited_9554_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(outsidebusiness_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(outsidebusiness_found_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(pst_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(pst_found_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(exam_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(exam_found_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(u4_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(u4_found_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(suitability_found_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(suitability_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(disqualification_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(disqualification_found_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(u4_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(u4_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`))  %>%
  left_join(just_and_equitable_alleged_time_series) %>%
  select(-c(`FALSE`, `TRUE`)) %>%
  left_join(just_and_equitable_found_time_series) %>%
  select(-c(`FALSE`, `TRUE`)) %>%
  left_join(expedited_time_series) %>%
  select(-c(`FALSE`, `TRUE`, `NA`))

total_count_stats <- total_count_summary %>%
  skim() 

total_count_stats %>%
  filter(skim_type == "numeric") %>%
  slice(2:26) %>%
  as_tibble() %>%
  mutate(`% complete` = round(complete_rate, digits = 2),
         Mean = round(numeric.mean, digits = 2),
         SD = round(numeric.sd, digits = 2),
         Minimum = round(numeric.p0, digits = 2),
         `25th %ile` = round(numeric.p25, digits = 2),
         `50th %ile` = round(numeric.p50, digits = 2),
         `75th %ile` = round(numeric.p75, digits = 2),
         Maximum = round(numeric.p100, digits = 2)) %>%
  select(skim_variable, `% complete`, Mean, SD, Minimum, `25th %ile`, `50th %ile`, `75th %ile`, Maximum, numeric.hist) %>%
  rename(Histogram = numeric.hist) %>%
  filter(!skim_variable %in% c("expedited_9552", "proportion_expedited_9554")) %>%
  regulartable() %>%
  save_as_docx(path = "Table 2.docx")

ggplot(data = total_count_summary, aes(x = year)) +
  #  geom_line() +
  geom_line(aes(y = proportion_8210_alleged, linetype = "Allegations of Rule 8210 violation"), 
            #colour = "chocolate3",
            linetype = "dotted",
            size = 1.5) +
  geom_line(aes(y = proportion_8210_found, linetype = "Findings of Rule 8210 violation"), 
            #colour = "magenta",
            linetype = "solid",
            size = 1.5) +
  geom_line(aes(y = proportion_expedited_9552, linetype = "Expedited proceedings under Rule 9552"), 
            colour = "magenta",
            linetype = "solid",
            size = 1.5) +
  labs(title = "Types of FINRA bars") +
  xlab("Year") +
  theme_minimal() +
  scale_y_continuous(name = "Annual percentage of bars in category",
                     labels = scales::label_percent())

ggsave("Figure 2 -- FINRA_8210_bars.png",
       dpi = 300,
       scale = 1)

library(forcats)

remedy_count_join <- total_count_for_scraping %>%
#  filter(!is.na(Resolution)) %>%
  mutate(year = year(eventDate)) %>%
  group_by(year) %>% 
  summarize(total = n())

unknowns <- total_count_for_scraping %>%
  filter(Resolution %in% c("Other", "Letter", "Missing Data"),
         is.na(eventDate)) %>%
  mutate(Resolution = fct("Unknown"),
         year = year(eventDate)) %>%
  group_by(year, Resolution) %>% 
  summarize(count = n())

expediteds <- total_count_for_scraping %>%
  filter(Resolution %in% c("Other", "Letter", "Missing Data"),
         !is.na(eventDate)) %>%
  mutate(Resolution = fct("Expedited Proceeding"),
         year = year(eventDate)) %>%
  group_by(year, Resolution) %>% 
  summarize(count = n())

unknowns <- unknowns %>%
  full_join(expediteds) %>%
  full_join(remedy_count_join) %>%
  mutate(proportion = count / total) %>%
  filter(!is.na(count),
         !is.na(year)) %>%
  print(n=Inf)

remedy_count <- total_count_for_scraping %>%
  mutate(year = year(eventDate)) %>%
  group_by(year, Resolution) %>% 
  summarize(count = n()) %>%
  full_join(remedy_count_join) %>%
  ungroup() %>%
  mutate(proportion = count / total) %>%
  full_join(unknowns) %>%
  filter(year>1998) 

remedy_count %>%
  filter(Resolution %in% c("Settlement", "Adjudication", "Expedited Proceeding")) %>%
  ggplot(aes(x = year, y = proportion, linetype = Resolution)) +
  geom_line() +
  labs(title = "Dispositions of FINRA bars (by proportion)") +
  xlab("Year") +
  theme_classic() +
  scale_y_continuous(name = "Percentage of that year's bars resolved by this category of disposition",
                     labels = scales::label_percent())

ggsave("Figure 7 --- FINRA_bars_dispositions_proportional.png",
       dpi = 300,
       scale = 1)

remedy_count %>%
  filter(Resolution %in% c("Settlement", "Adjudication", "Expedited Proceeding")) %>%
  ggplot(aes(x = year, y = count, linetype = Resolution)) +
  geom_line() +
  labs(title = "Dispositions of FINRA bars (count)") +
  xlab("Year") +
  theme_classic() +
  scale_y_continuous(name = "Annual count of bars resolved by this category of disposition")

ggsave("Figure 8 --- FINRA_bars_dispositions_count.png",
       dpi = 300,
       scale = 1)

remedy_count %>%
  filter(Resolution %in% c("Expedited Proceeding", "Missing Data", "Letter")#, "Other")
         ) %>%
  mutate(Resolution = fct_recode(Resolution, "Expedited Proceeding" = "Robustness")) %>%
  ggplot(aes(x = year, y = proportion, linetype = Resolution)) +
  geom_line() +
  geom_line(data = total_count_summary, aes(x = year, y = proportion_expedited_9552), 
            colour = "magenta",
            linetype = "solid") +
  labs(title = "Robustness check: Brokercheck 'resolution' categories identify expedited proceedings",
       subtitle = "Collapsing 'letter', 'other', and NA approximates regex match for Rule 9552 (magenta)") +
  xlab("Year") +
  theme_classic() +
  scale_y_continuous(name = "Annual percentage of bars in category",
                     labels = scales::label_percent())

ggsave("Figure 9 --- FINRA_bars_robustness_proportions.png",
       dpi = 300,
       scale = 1)

remedy_count %>%
  filter(Resolution %in% c("Letter", "Other", "Missing Data", "Expedited Proceeding")) %>%
  mutate(Resolution = fct_recode(Resolution, "Expedited Proceeding" = "Robustness")) %>%
  ggplot(aes(x = year, y = count, linetype = Resolution)) +
  geom_line() +
  geom_line(data = total_count_summary, aes(x = year, y = expedited_9552), 
            colour = "magenta",
            linetype = "solid") +
  labs(title = "Robustness check: Brokercheck 'resolution' categories identify expedited proceedings",
       subtitle = "Collapsing 'letter', 'other', and NA approximates regex match for Rule 9552 (magenta)") +
  xlab("Year") +
  theme_classic() +
  scale_y_continuous(name = "Annual count of bars in category")

ggsave("Figure 10 --- FINRA_bars_robustness_counts.png",
       dpi = 300,
       scale = 1)




ggplot(data = total_count_summary, aes(x = year)) +
  geom_line(aes(y = proportion_conversion_alleged), 
            linetype = "dashed",
            colour = "cyan4",
            size = 1.5) +
  geom_line(aes(y = proportion_fraud_alleged), 
            linetype = "21",
            colour = "grey85",
            size = 1.5) +
#  geom_line(aes(y = proportion_outsidebusiness_alleged), 
#            linetype = "21",
#            colour = "grey15",
#            size = 1.5) +
#  geom_line(aes(y = proportion_not_accounted_for), 
#            linetype = "21",
#            colour = "red",
#            size = 1.5) +
  geom_line(aes(y = proportion_just_and_equitable_alleged), 
            #            linetype = "21",
            colour = "green",
            size = 1.5) +
  geom_line(aes(y = proportion_expedited_9552), 
            colour = "magenta",
            linetype = "solid",
            size = 1.5) +
#  geom_line(aes(y = proportion_pst_alleged), 
            #            linetype = "21",
#            colour = "blue",
#            size = 1.5) +
#  geom_line(aes(y = proportion_u4_alleged),
#            colour = "orange",
#            size = 1.5) +
#  geom_line(aes(y = proportion_suitability_alleged), 
#            #            linetype = "21",
#            colour = "black",
#            size = 1.5) +
#  geom_line(aes(y = proportion_exam_alleged), 
            #            linetype = "21",
#            colour = "wheat3",
#            size = 1.5) +
  labs(title = "Types of FINRA bars (by subject matter alleged)") +
  xlab("Year") +
  theme_minimal() +
  scale_y_continuous(name = "Annual percentage of bars in category",
                                                  labels = scales::label_percent())

ggsave("Figure 5 --- FINRA_bars_allegations.png",
       dpi = 300,
       scale = 1)

ggplot(data = total_count_summary, aes(x = year)) +
  #  geom_line() +
  geom_line(aes(y = proportion_conversion_found), 
            linetype = "dashed",
            colour = "cyan4",
            size = 1.5) +
  geom_line(aes(y = proportion_fraud_found), 
            linetype = "21",
            colour = "grey85",
            size = 1.5) +
    geom_line(aes(y = proportion_just_and_equitable_found), 
  #            #            linetype = "21",
              colour = "green",
              size = 1.5) +
  geom_line(aes(y = proportion_suitability_found), 
            #            linetype = "21",
            colour = "black",
            size = 1.5) +
#  geom_line(aes(y = proportion_exam_found), 
#            #            linetype = "21",
#            colour = "wheat3",
#            size = 1.5) +
  labs(title = "Types of FINRA bars (by conduct found)") +
  xlab("Year") +
  theme_minimal() +
  scale_y_continuous(name = "Annual percentage of bars in category",
                     labels = scales::label_percent())

ggsave("Figure 6 --- FINRA_bars_findings.png",
       dpi = 300,
       scale = 1)



without_admitting_time_series <- total_count_for_scraping %>%
  group_by(withoutadmitting, year) %>%
  count() %>%
  pivot_wider(names_from = withoutadmitting, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_withoutadmitting = `TRUE` / (`FALSE` + `TRUE`))

awc_time_series <- total_count_for_scraping %>%
  group_by(by_consent_detected, year) %>%
  count() %>%
  pivot_wider(names_from = by_consent_detected, values_from = n) %>%
  ungroup() %>%
  mutate(proportion_by_consent_detected = `TRUE` / (`FALSE` + `TRUE`))

adj_time_series <- remedy_count %>%
  filter(Resolution %in% c("Adjudication")) %>%
  select(-c(Resolution, count, total)) %>%
  rename(proportion_adjudicated = proportion)

awc_bars <- ft_summarize_count %>%
  rename(year = Year) %>%
  left_join(awc_time_series) %>%
  select(-c(`FALSE`, `TRUE`)) %>%
  left_join(without_admitting_time_series) %>%
  select(-c(`FALSE`, `TRUE`)) %>%
  left_join(adj_time_series) %>%
  ggplot(aes(x = year)) +
  geom_line(aes(y = proportion_by_consent_detected), linetype = "solid", size = 1) +
  geom_line(aes(y = proportion_withoutadmitting), linetype = "dotted", size = 1) +
  xlab("Year") +
  scale_y_continuous("Proportion of bars",
                     labels = scales::label_percent()) +
  theme_classic()

awc_bars +   
  labs(title = "FINRA bars resolved by consent or settlement")

ggsave("Figure 3 --- FINRA_awc_bars.png",
       dpi = 300,
       scale = 1)

awc_bars +
  # If you want, you can include a version of this that has 
  labs(title = "FINRA bars resolved by consent or settlement, vs. adjudications") +
  geom_line(aes(y = proportion_adjudicated), linetype = "solid", color = "orange", size = 1)

ggsave("Figure 3 variation --- FINRA_awc_bars.png",
       dpi = 300,
       scale = 1)




# run!
rule_8210_alleged_time_series %>%
  filter(year > 1999) %>%
  ggplot(aes(x = year)) +
  geom_line(aes(y = `FALSE`), colour = 'grey70') +
  geom_line(aes(y = `TRUE`)) +
  geom_line(aes(y = `TRUE` + `FALSE`), linetype = "dashed") +
  geom_line(data = (rule_8210_alleged_time_series %>% filter(year > 1999)), 
            aes(y = proportion_8210_alleged * 400), 
            linetype = "dotted",
            colour = "chocolate3", 
            size = 1.5) +
#  geom_line(data = total_count, aes(x = year, y = count)) +
  labs(title = "FINRA bars involving rule 8210"#,
       #caption = "Black line is count of 8210 bars, grey line is non-8210 count. Dashed line is total. Dotted is proportion of 8210. All series binned by year."
       ) +
  xlab("Year") +
  ylab("Number of bars in category") +
  theme_minimal() +
  scale_y_continuous("Annual count",
                     sec.axis = ggplot2::sec_axis(~. / 400,
                                                  name = "Annual percentage",
                                                  labels = scales::label_percent()))

ggsave("FINRA_8210_bars_2.png",
       dpi = 300,
       scale = 0.5)



# this was exploratory --- did not use

total_count_for_scraping %>%
  select(CRD, Allegations, conversion_found, conversion_alleged, year) %>%
  group_by(conversion_found, year) %>%
  count() %>%
  pivot_wider(names_from = conversion_found, values_from = n) %>%
  filter(year > 1999,
         year < 2023) %>%
  mutate(proportion = `TRUE` / (`FALSE` + `TRUE`)) %>%
  ggplot(aes(x = year)) +
  geom_line(aes(y = `FALSE`), colour = 'grey70') +
  geom_line(aes(y = `TRUE`)) +
  geom_line(aes(y = proportion * 3000), linetype = "dotted") +
  labs(title = "FINRA bars involving conversion") +
  xlab("Year") +
  ylab("Number of bars in category") +
  theme_minimal() +
  scale_y_continuous("Annual count",
                     sec.axis = ggplot2::sec_axis(~. / 3000,
                                                  name = "Annual percentage",
                                                  labels = scales::label_percent()))


# i don't think we use this but it's worth checking out

total_count_for_scraping %>%
  select(CRD, Allegations, by_consent_detected, year) %>%
  group_by(by_consent_detected, year) %>%
  count() %>%
  pivot_wider(names_from = by_consent_detected, values_from = n) %>%
  filter(year > 1999,
         year < 2023) %>%
  mutate(proportion = `TRUE` / (`FALSE` + `TRUE`)) %>%
  ggplot(aes(x = year)) +
  geom_line(aes(y = `FALSE`), colour = 'grey70') +
  geom_line(aes(y = `TRUE`)) +
  geom_point(aes(y = proportion * 1000)) +
  labs(title = "FINRA bars resolved by consent",
       subtitle = "In recent years, just barely a majority of bars annually") +
  xlab("Year") +
  ylab("Number of bars in category") +
  theme_minimal() +
  scale_y_continuous("Annual count",
                     sec.axis = ggplot2::sec_axis(~. / 1000,
                                                  name = "Annual percentage",
                                                  labels = scales::label_percent()))
  
saveRDS(total_count_for_scraping, "total_count_for_scraping_post_analysis_2024_03_03.RDS")



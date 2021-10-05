library(ggplot2)
library(panelvar)
library(tidyverse)

dir <- 'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet/brazil/'
fn <- paste0(dir, 'brazil_reg_data.csv')
data <- read.csv(fn)

#  pick relevant cols and drop duplicates.
keep_cols <- c('ISIN', 'date', #'yield.bid', 'price.bid',
               'asset.code',
               'qtm', 'type',
               'p.mean', 'y.mean',
               'p.sd', 'y.sd',
               'issue_px', 'issue_yd',
               'mat_px', 'mat_yd',
               'mat_mo',
               'hhi')
data <- data[,keep_cols]
df <- unique(data)
df <- df[df$ISIN != '',]
unique(df$date)
# data <- data[duplicated(data),]

## panel var with fe. 
# data(Cigar)
# ex1_feols <-
#   pvarfeols(dependent_vars = c("log_sales", "log_price"),
#             lags = 1,
#             exog_vars = c("cpi"),
#             transformation = "demean",
#             data = Cigar,
#             panel_identifier= c("state", "year"))
# summary(ex1_feols)

feols <-
  pvarfeols(dependent_vars = c("p.sd", "y.sd"),
            lags = 1,
            exog_vars = c("qtm", 
                          # "issue_px", 
                          # "mat_px",
                          # "issue_yd", 
                          # "mat_yd",
                          "hhi"
                          ),
            transformation = "demean",
            data = df,
            panel_identifier= c("ISIN", "date"))
summary(feols)

data("Dahlberg")
## Not run:
ex1_dahlberg_data <- pvargmm(dependent_vars = c("expenditures", "revenues", "grants"),
                             lags = 1,
                             transformation = "fod",
                             data = Dahlberg,
                             panel_identifier=c("id", "year"),
                             steps = c("twostep"),
                             system_instruments = FALSE,
                             max_instr_dependent_vars = 99,
                             max_instr_predet_vars = 99,
                             min_instr_dependent_vars = 2L,
                             min_instr_predet_vars = 1L,
                             collapse = FALSE
)
summary(ex1_dahlberg_data)

Dahlberg
df
dim(data[data$ISIN == '',])
dim(data)
dim(df[df$ISIN == '',])

gmmfe <-
  pvargmm(dependent_vars = c("p.sd", 'p.mean', 'qtm'),
          lags = 2,
          transformation = "fod",
          data = df,
          panel_identifier= c("ISIN", "date"),
          steps = c("twostep"),
          # system_isntruments = FALSE,
          # max_instr_dependent_vars = 99,
          # max_instr_predet_vars = 99,
          # min_instr_dependent_vars = 2L,
          # min_instr_predet_vars = 1L,
          collapse = FALSE
          )
summary(gmmfe)

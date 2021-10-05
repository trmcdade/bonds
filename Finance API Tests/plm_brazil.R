rm(list=ls())
# install.packages('plm')
library(plm)
# library(tidyverse)
# library(dplyr)

dir <- 'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet/brazil/'
fn <- paste0(dir, 'brazil_reg_data.csv')
data <- read.csv(fn)
head(data)

#  pick relevant cols and drop duplicates.
keep_cols <- c('ISIN', 'date', 
               #'yield.bid', 'price.bid',
               # 'asset.code',
               'qtm', 
               'mty_length_days',
               'type',
               'p.mean', 
               # 'y.mean',
               'p.sd', 
               'y.sd',
               'issue_px',
               'issue_yd',
               'mat_px',
               'mat_yd',
               # 'mat_mo',
               'hhi')
data <- data[,keep_cols]
df <- unique(data)
df <- df[df$ISIN != '',]
df <- df[!is.na(df$hhi),]
dim(unique(df[,c('ISIN', 'date')]))
dim(df)
df <- df[!duplicated(df[,c('ISIN', 'date')]) ,]

data("EmplUK", package = "plm")
EmplUK
z2 <- pgmm(log(emp) ~ 
             lag(log(emp), 1) + 
             lag(log(wage), 0:1) +
             lag(log(capital), 0:1) | 
             lag(log(emp), 2:99) +
             lag(log(wage), 2:99) + 
             lag(log(capital), 2:99),        
           data = EmplUK, 
           effect = "twoways", 
           model = "onestep", 
           transformation = "ld")
summary(z2, robust = TRUE)

## Blundell and Bond (1998) table 4 (cf. DPD for OX p. 12 col. 4)
df <- pdata.frame(df, index = c('ISIN', 'date'))
pdim(df)
# plot(df$date, df$p.mean)
# hist(df[,'qtm'], breaks = 120)

mols <- plm(p.sd ~ 
              1 + 
              type + 
              issue_px + 
              mty_length_days +
              # issue_yd + 
              # mat_px +
              # mat_yd +
              lag(qtm) +
              lag(p.mean) +
              lag(hhi), 
            data = df, 
            model = 'within', 
            effect = 'time')
summary(mols)

# add isin effects
m_2w <- plm(p.sd ~ 
              1 + 
              type + 
              mty_length_days +
              issue_px + 
              issue_yd + 
              mat_px +
              mat_yd +
              lag(qtm) + 
              lag(p.mean) + 
              lag(hhi), 
            data = df, 
            model = 'within', 
            effect = 'twoway')
summary(m_2w)

# Anderson-Hsiao
m_ah <- plm(p.sd ~ 
              1 + 
              type + 
              mty_length_days +
              issue_px +
              issue_yd + 
              # mat_px +
              # mat_yd + 
              lag(diff(qtm)) + 
              lag(diff(p.mean)) + 
              lag(diff(hhi)) +
              date | 
              1 + 
              lag(qtm, 2) +
              lag(hhi, 2) +
              lag(p.mean, 2) * date, 
            data = df, 
            model = 'pooling')
summary(m_ah)

m_ab <- pgmm(p.sd ~
               lag(qtm) +
               lag(p.mean) +
               lag(hhi) |
               lag(qtm, 2) + lag(p.mean, 2:10) + lag(hhi, 2),
             # random.method = "walhus",
             data = df,
             # index = c('ISIN', 'date'),
             effect = "twoway", 
             model = "onestep", 
             # collapse = TRUE,
             transformation = "d"
             )
summary(m_ab)

m_bb <- pgmm(p.sd ~
               lag(qtm) +
               lag(p.mean) +
               lag(hhi) |
               lag(qtm, 2) | 
               lag(p.mean, 2:99) |
               lag(hhi, 2),
             # random.method = "walhus",
             data = df,
             # index = c('ISIN', 'date'),
             effect = "twoway", 
             model = "onestep", 
             # collapse = TRUE,
             transformation = "ld"
)


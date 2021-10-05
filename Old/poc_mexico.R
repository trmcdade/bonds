rm(list=ls())

# Function to load packages
loadPkg = function(toLoad){
  for(lib in toLoad){
    if(! lib %in% installed.packages()[,1])
    { install.packages(lib, repos='http://cran.rstudio.com/') }
    suppressMessages( library(lib, character.only = TRUE) ) }
}

#load these libraries
packs = c('ggplot2', 'dplyr', 'scales', 'haven', 'readstata13',
          'tictoc', 'RColorBrewer', 'zoo', 'lmtest', 'margins',
          'stargazer', 'bvartools', 'vars', 'urca', 'reshape2', 
          'dynlm', 'tseries', 'car', 'fUnitRoots', 'panelvar',
          'plm', 'gplots', 'xtable', 'lme4', 'ecm', 'MuMIn', 
          'lmerTest', 'dotwhisker', 'margins', 'readxl', 'stringr')

loadPkg(packs)
set.seed(619)
dir <- "C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/Bloomberg"
setwd(dir)

## for the summed at currency level:
data <- read.csv("for_reg.csv",
                   stringsAsFactors = TRUE,
                   # sheet = 2,
                   # row.names = 1,
                   header = TRUE
                   )
data <- data[(data$HHI <= 100) & (data$HHI != 0),]
industries <- c('Sovereigns', 'Government Development Banks',
                'Government Regional', 'Government Agencies')
data <- data[data['INDUSTRY'] == industries[1],]


head(data)
dim(data)
# data <- data[,1:(dim(data)[2] - 3)]
data %>% 
  mutate(BB_COMPOSITE = str_replace(BB_COMPOSITE, "#N/A N/A", "NR"))
# data[data == "#N/A N/A" ] <- NA
data[,'CUR_YLD'] <- as.numeric(as.matrix(data[,'CUR_YLD']))
data[,'YTM'] <- as.numeric(as.matrix(data[,'YTM']))
data[,'ISSUE_PX'] <- as.numeric(as.matrix(data[,'ISSUE_PX']))
data[,'PX_LAST'] <- as.numeric(as.matrix(data[,'PX_LAST']))
data[,'PX_DELTA'] <- data[,'REDEMP_VAL'] - data[,'PX_LAST']

data[,'DV'] <- data[,'YTM'] - data[,'CPN']
# This is the amount by which the YTM (the market's speculative rate of return 
# on the bond if held until maturity) is larger than the yield at issuance.
# this is really what I want. 

hist(data[, 'HHI'], breaks = 100)
hist(data[, 'CPN'], breaks = 100)
hist(data[, 'Mty..Yrs.'], breaks = 100)
hist(data[, 'CUR_YLD'], breaks = 100)
hist(data[, 'AMT_OUTSTANDING'], breaks = 100)
hist(data[, 'DIFF'], breaks = 100)
hist(data[, 'PX_LAST'], breaks = 100)

m_cpn <- lm(DV ~ 
              AMT_OUTSTANDING +
              CPN_TYP +
              TICKER +
              # CURR_TYPE +
              # BB_COMPOSITE +
              # Mty..Yrs. +
              PX_DELTA + 
              HHI
              # HHI * AMT_OUTSTANDING
            , data = data
            )
summary(m_cpn)

m <- margins(m_cpn)
summary(m)
plot(m)

# a one-pt increase in HHI corresponds to a 0.9822% decrease in DIFF, with low 
# statistical confidence. This means that more concentration means closer to 
# the yield at issue. Can't be right. 
# This effect is made stronger and more stat significant by excluding credit 
# rating, ticker symbol, mty (yrs), or coupon type, alone or together.
# why could this be? YTM incorporates 
# a price more similar to face value (lower diff) results in higher coupon. 
# 

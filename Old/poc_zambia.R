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
dir <- "C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet"
setwd(dir)

## for the summed at currency level:
data <- read.csv("zambia_reg_data.csv",
                   stringsAsFactors = TRUE,
                   # sheet = 2,
                   row.names = 1,
                   header = TRUE
                   )
data <- data[(data$hhi <= 100) & (data$hhi != 0),]
# industries <- c('Sovereigns', 'Government Development Banks',
                # 'Government Regional', 'Government Agencies')
# data <- data[data['INDUSTRY'] == industries[1],]

head(data)
dim(data)
# data <- data[,1:(dim(data)[2] - 3)]
# data %>% 
  # mutate(BB_COMPOSITE = str_replace(BB_COMPOSITE, "#N/A N/A", "NR"))
# data[data == "#N/A N/A" ] <- NA
unique(data[,'issue_px'])
data[,'issue_px'] <- as.numeric(as.matrix(data[,'issue_px']))
data[,'px_delta'] <- data[,'redemp_val'] - data[,'px_last']
data[,'dv'] <- data[,'yld_ytm_bid'] - data[,'cpn']
# This is the amount by which the YTM (the market's speculative rate of return 
# on the bond if held until maturity) is larger than the yield at issuance.
# this is really what I want. 

hist(data[, 'hhi'], breaks = 100)
hist(data[, 'cpn'], breaks = 100)
hist(data[, 'Mty..Yrs.'], breaks = 100)
hist(data[, 'yld_cur_mid'], breaks = 100)
hist(data[, 'amt_outstanding'], breaks = 100)
hist(data[, 'px_delta'], breaks = 100)
hist(data[, 'dv'], breaks = 100)

head(data)

m_cpn <- lm(dv ~ 
              # amt_outstanding +
              cpn_typ +
              ticker +
              currency + 
              # curr_type +
              # bb_composite +
              # include some sort of price control? does that make sense? 
              px_last +
              # px_delta +
              # hhi
              hhi * amt_outstanding
            , data = data
            )
summary(m_cpn)

m <- margins(m_cpn)
summary(m)
plot(m)

# a one-pt increase in HHI corresponds to a 307.3 increase in DV, the difference 
# between the secondary market YTM and face value, with fluctuating  
# statistical confidence. 
# This means that a concentrated owner base corresponds to a bigger yield premium. 
# There is a small interaction effect with amt_out. This says that 
# the effect of hhi on the yield spread (the dv) decreases when there is more
# debt outstanding. Does this make theoretical sense?


# This effect is made stronger and more stat significant by excluding credit 
# rating, ticker symbol, mty (yrs), or coupon type, alone or together.
# why could this be? YTM incorporates 
# a price more similar to face value (lower diff) results in higher coupon. 
# 

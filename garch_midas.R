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
          'tictoc', 'RColorBrewer', 'zoo', 'glue', 'devtools', 
          'sandwich', 'plm', 'lme4', 'lmtest', 'lmerTest', 
          'margins', 'mfGARCH', 'stargazer', 'xtable',
          'dotwhisker', 'broom', 'coefplot', 'pROC', 'MuMIn'
          )
loadPkg(packs)

set.seed(619)
dir <- "C:/Users/trmcd/OneDrive/Duke/Papers/Bonds/Data/FactSet/"
setwd(dir)

data <- read.csv("CA_reg_data_2021-09-30.csv",
                  # stringsAsFactors = TRUE, 
                 stringsAsFactors = FALSE,
                 row.names = 1,
                  header = TRUE)
head(data)
data$date <- as.Date(data$date, format = "%Y-%m-%d")
data$date_q <- as.Date(data$date_q, format = "%Y-%m-%d")
data <- data[order(data$CUSIP, data$date),]
data <- data[(!is.na(data$hhi))
             & (data$hhi <= 1)
             & (data$pct_os_known <= 1)
             ,]
head(data)

#################################
##### GARCH-MIDAS APPROACH ######
#################################

# https://github.com/onnokleen/mfGARCH/
# The example from Conrad & Kleen 2020. 
# head(df_financial)
# fit_mfgarch(data = df_financial, y = "return", x = "nfci", low.freq = "week", K = 52)

# My example. 
keep_cols <- c('CUSIP', 'date', 'date_q', 'log_ret', 'log_ret_100',
               'vol_3d', 'vol_5d', 'vol_10d', 'vol_30d', 'vol_60d', 'vol_90d', 
               # 'vol_3d_q', 'vol_5d_q', 'vol_10d_q', 'vol_30d_q', 'vol_60d_q', 'vol_90d_q', 
               'hhi', 'qtm') #, 'CUSIP')
keep_cusips <- c()
n_hhi <- c()
n_dates <- c()
ordered_cusips <- unique(data[order(-data$amt_outstanding),'CUSIP'])
for (cc in ordered_cusips) {
  temp <- data[data$CUSIP == cc, ]
  if (length(unique(temp$hhi)) > 1) {
    keep_cusips <- append(keep_cusips, cc)
    n_hhi <- append(n_hhi, length(unique(temp$hhi)))
    n_dates <- append(n_dates, length(unique(temp$date_q)))
  }
}
diagnostics <- data.frame(keep_cusips, n_hhi, n_dates)
diagnostics <- diagnostics[order(-diagnostics$n_dates, -diagnostics$n_hhi),]
head(diagnostics)

data$log_ret_100 <- data$log_ret * 100
head(data)
hist(diagnostics$n_hhi,
     breaks = 30, 
     xlab = 'Number of Unique HHI per CUSIP', ylab = 'Frequency',
     main = 'Histogram of Variation in HHI by CUSIP')

plot(diagnostics$n_dates, diagnostics$n_hhi)

diagnostics <- diagnostics[order(-diagnostics$n_hhi, -diagnostics$n_dates),]
long_cusips <- as.vector(diagnostics[1:10, 'keep_cusips'])
diagnostics
i <- 1
long_cusips[i]
diagnostics[diagnostics$keep_cusips == long_cusips[i],]
k <- diagnostics[diagnostics$keep_cusips == long_cusips[i], 'n_dates'] - 3
gm <- fit_mfgarch(data = data[data$CUSIP == long_cusips[i],keep_cols]
              ,y = "log_ret" 
              ,x = "hhi"
              ,low.freq = "date_q" 
              ,K = 25
              # ,weighting = 'beta.unrestricted' # unrestricted if weights are hump-shaped. restricted is default.
              # ,gamma = FALSE # TRUE means short term estimated by asymmetric GJR-GARCH. FALSE means GARCH(1,1).
              # ,x.two = 'qtm'
              # ,low.freq.two = 'date_q'
              # ,K.two = 00
              # ,weighting.two = ''
              # ,multi.start = FALSE
              )
gm
plot_weighting_scheme(gm) # max of this outputted graph is the value of K. 
# output a table for the write-up.
gm$variance.ratio
print(xtable(gm$broom.mgarch, digits = 3), include.rownames = FALSE)


# from wang et al 2019 (p.688): 
# parameters positive and significant means that the model fits and can be 
# used to forecast volatility. 
# sums of a and b are close to 1, confirming existence of a strong
# volatility persistence effect
# significant negative gamma parameter means that lower indep var has a 
# greater short-term effect on volatility than an increase does.
# negative significant theta terms for long term volatility mean lower long
# term volatility and positive mean high long-term volatilities.
# next, divide the one into training and predict sets based on time. 
# use MSE, MAE, HMSE, HMAE, QLIKE
# highly significant gamma parameter means that there is strong evidence for 
# asymmetry (p36). 
# a and b add up to 1, which is conventional in garch literature. 
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

hist(data$hhi,
     breaks = 100, 
     xlab = 'Ownership Concentration (HHI)', ylab = 'Frequency',
     main = 'Histogram of Ownership Concentration')

hist(data$pct_os_known,
     breaks = 100, 
     xlab = '% Known of Outstanding Debt', ylab = 'Frequency',
     main = 'Histogram of Percent Ownership Known')

# linear models are not defensible because of serial correlation. I need to 
# use a time-series model that adjusts for that sort of thing. Consider 
# something from the plm package. 
# Alternatively, try first-differencing the data. 
# use hausman-type tests to establish fixed or random effects. 
# assume some variance of the coefficient across i and t. want an 
# unrestricted or general GLS.

# Use pct os known as measure of confidence in hhi. Then for higher values of 
# posk I want to have a higher weight given to hhi. for lower values of posk
# I want to have lower weight given to hhi. Maybe I can use GT on this. 

pdat <- pdata.frame(data, index=c("CUSIP", "date"))
dvs <- c('vol_3d_q', 'vol_5d_q','vol_10d_q', 'vol_30d_q', 'vol_60d_q', 'vol_90d_q')

test <- plm(vol_10d_q ~
              hhi
            + pct_os_known
            + BBG.Composite
            + amt_outstanding * still_outstanding
            # + Issue.Price
            + Yield.at.Issue
            + Cpn
            + Coupon.Type
            + mty_typ
            + Mty..Yrs.
            + Mty.Size
            # + dtm
            + poly(qtm, 2)
            # + qtm * Mty..Yrs.
            + close_px_q
            , data = pdat
            , model = "within"
            , effects = 'indiv')
summary(test)

############################
##### WiTHIN APPROACH ######
############################

ml <- rep(list(NA),7)
for (dv in dvs) {
  index <- which(dvs == dv)
  dd <- unlist(strsplit(strsplit(dv, '_')[[1]][2], 'd'))
  f <- glue('{dv} ~ hhi + pct_os_known+ BBG.Composite + amt_outstanding * still_outstanding + Yield.at.Issue + Cpn + Coupon.Type + mty_typ + Mty..Yrs.+ Mty.Size+ + poly(qtm, 2) + close_px_q')
  f <- as.formula(f)
  mlp <- plm(f
             , data = pdat
             , model = 'within'
             # , model = 'fd'
             , effect = 'indiv'
             # , effect = 'twoways'
  )
  ml[[index]] <- mlp
}

for (mm in ml) {
  print(summary(mm))
}
omit_vars <- c()
cleancols <- c("HHI", "Pct. OS Known", "Quarters to Maturity", 
               "Quarters to Maturity Sq.", "Close Price")
stargazer(ml[[1]], ml[[2]], ml[[3]], ml[[4]], ml[[5]], ml[[6]],
          column.labels = c('3d', '5d', '10d', '30d', '60d', '90d'),
          dep.var.labels.include = FALSE,
          dep.var.caption = 'DV: Number of Days Rolling Volatility $v^{nq}_{it}$', 
          covariate.labels = cleancols,
          colnames = FALSE,
          df = FALSE,
          digits = 4,
          font.size = "small",
          column.sep.width = "-5pt",
          omit = c('Constant', 'aic', 'bic', 'll', omit_vars),
          no.space = TRUE,
          header = FALSE, 
          type = 'latex'
)


################################
##### FIRST DIFF APPROACH ######
################################

ml_fd <- rep(list(NA),7)
for (dv in dvs) {
  index <- which(dvs == dv)
  dd <- unlist(strsplit(strsplit(dv, '_')[[1]][2], 'd'))
  f <- glue('{dv} ~ hhi + pct_os_known+ BBG.Composite + amt_outstanding * still_outstanding + Yield.at.Issue + Cpn + Coupon.Type + mty_typ + Mty..Yrs.+ Mty.Size+ poly(qtm, 2) + close_px_q')
  f <- as.formula(f)
  mlp <- plm(f
             , data = pdat
             # , model = 'within'
             , model = 'fd'
             , effect = 'indiv'
             # , effect = 'twoways'
  )
  ml_fd[[index]] <- mlp
}

for (mm in ml_fd) {
  print(summary(mm))
}

omit_vars <- c()
cleancols <- c("HHI", "Pct. OS Known", "Quarters to Maturity",
               "Quarters to Maturity Sq.", "Close Price")
stargazer(ml_fd[[1]], ml_fd[[2]], ml_fd[[3]], ml_fd[[4]], ml_fd[[5]], ml_fd[[6]],
          column.labels = c('3d', '5d', '10d', '30d', '60d', '90d'),
          dep.var.labels.include = FALSE,
          dep.var.caption = 'DV: Number of Days Rolling Volatility $\\Delta v^{nq}_{it}$', 
          covariate.labels = cleancols,
          colnames = FALSE,
          df = FALSE,
          digits = 4,
          font.size = "small",
          column.sep.width = "-5pt",
          omit = c('Constant', 'aic', 'bic', 'll', omit_vars),
          no.space = TRUE,
          header = FALSE, 
          type = 'latex'
)



# positive coefficient means that higher HHI (more concentrated) means 
# higher volatility.

# find the gamma and nu of the unbalanced panel. 1 represents balanced, 0 unbalanced. 
# Pretty unbalanced. 
punbalancedness(mlp)
# test for twoway effects. result: there are twoway effects. 
pooling <- plm(vol_10d ~ 
                 hhi 
               + pct_os_known
               + BBG.Composite
               + amt_outstanding * still_outstanding
               + Issue.Price
               + Yield.at.Issue
               + Cpn
               + Coupon.Type
               + mty_typ
               + Mty..Yrs.
               + Mty.Size
               + dtm
               + qtm
               + close_px
               , data = pdat
               , model="pooling")
plmtest(pooling, effect = 'twoways', type = 'ghm')
# test for individual effects
pFtest(mlp, pooling)
# test for cross-sectional dependence. 
pcdtest(mlp, test = 'sclm')



#############################
##### MARGINS APPROACH ######
#############################

ml <- lmer(vol_10d_q ~ 
             hhi 
           + pct_os_known
           + qtm + I(qtm^2)
           + close_px 
           + (1 | CUSIP)
           , data = data)
summary(ml)
m <- margins(ml, at = list(qtm = fivenum(data$qtm, na.rm = TRUE)))
# cplot(ml, x = "qtm", dx = "qtm", what = "effect", se.type = "shade")
summary(margins(ml))
cplot(ml, x = "I(qtm^2)", what = "effect", se.type = "shade")


summary(ml[[1]])
summary(margins(ml[[1]]))
marg <- margins(ml[[1]], at = list(qtm = fivenum(pdat$qtm, na.rm = TRUE)))
sm <- summary(m)
sm <- data.frame(lapply(sm, function(y) if(is.numeric(y)) round(y, 8) else y)) 
sm <- sm[sm['factor'] =='Months.Mat.To.Election',]
sm[,'factor'] <- 'Months To Election'
names(sm)[2] <- 'Legislative Vote Margin'

# output a table for the write-up.
print(xtable(sm[,2:dim(sm)[2]], digits = 3), include.rownames = FALSE)

# output a plot for the write-up. 
ggplot(data = sm) + 
  theme_bw() +
  geom_line(aes(x = `Legislative Vote Margin`, y = AME), colour = new_colors[1]) +
  geom_ribbon(aes(x = `Legislative Vote Margin`, y = AME,
                  ymin = lower, ymax = upper),
              linetype = 2, alpha = .15, colour = new_colors[1]) +
  scale_color_manual(values=new_colors) +
  theme(plot.title = element_text(hjust = 0.5),
        text = element_text(size=18),
        legend.position = 'bottom',
        legend.title = element_blank()) +
  labs(colour = "Variable:") +
  xlab("Executive Vote Share") +
  ylab('Marginal Effect of Months Maturity \n to Election on Share') #+
ggsave(paste(dir, glue('/cmcm_ame_24m_usd_{var}.png'), sep = ''))


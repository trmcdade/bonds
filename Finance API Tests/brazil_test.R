# install.packages('GetTDData')
library(GetTDData) #downloads bond pricing data from the Brazilian Treasury. 
library(ggplot2)
library(lubridate)
library(dplyr)
library(zoo)

types <- c('LFT', 'LTN', 'NTN-C', 'NTN-B', 'NTN-B Principal', 'NTN-F')
asset.codes <- types   # Identifier of assets
maturity <- NULL  # Maturity date as string (ddmmyy)
# download the data. only need to do this once. 
# my.flag <- download.TD.data(asset.codes = asset.codes)
# read the files. There's a problem with NTN-C_2012.xls, so see if you can find it online? 
data <- read.TD.files(asset.codes = asset.codes, maturity = maturity)
# head(data)

data$type <- gsub(" [^ ]*$", "", data$asset.code)
data$mat_year <- sapply(strsplit(as.character(data$matur.date), '-'), "[", 1)
data$mat_month <- sapply(strsplit(as.character(data$matur.date), '-'), "[", 2)
data$mat_day <- sapply(strsplit(as.character(data$matur.date), '-'), "[", 3)
data$ttm <- sapply(difftime(time1 = data$matur.date, time2 = data$ref.date, units = "days"), "[")
data$qtm <- data$ttm / (365.25/4)
data$yymm <- gsub("-", "",gsub("[^-]*$", "", data$ref.date))
yq <- as.yearqtr(as.character(data$yymm), "%Y%m")
data$yyqq <- gsub(" ", "-", yq)

# head(data)
# hist(data$yield.bid)
# hist(data[data$typ == types[6], 'yield.bid'])

out <- data.frame()
for (ac in unique(data$asset.code)){
  out_ac <- data.frame()
  temp <- data[data$asset.code == ac,]
  temp <- temp[order(temp$ref.date),]
  issue.date <- temp[1,'ref.date']
  temp$issue.date <- issue.date
  for (dd in unique(temp$yyqq)) {
    t <- temp[temp$yyqq == dd,]
    t$p.sd <- sd(t$price.bid)
    t$p.mean <- mean(t$price.bid, na.rm = TRUE)
    t$y.sd <- sd(t$yield.bid)
    t$y.mean <- mean(t$yield.bid, na.rm = TRUE)
    out_ac <- rbind(out_ac, t)
  }
  # below normalizes the four main vars. 
  out_ac$p.mean <- (out_ac$p.mean - mean(out_ac$p.mean, na.rm = TRUE)) / 
    sd(out_ac$p.mean, na.rm = TRUE)
  out_ac$p.sd <- (out_ac$p.sd - mean(out_ac$p.sd, na.rm = TRUE)) / 
    sd(out_ac$p.sd, na.rm = TRUE)
  out_ac$y.mean <- (out_ac$y.mean - mean(out_ac$y.mean, na.rm = TRUE)) / 
    sd(out_ac$y.mean, na.rm = TRUE)
  out_ac$y.sd <- (out_ac$y.sd - mean(out_ac$y.sd, na.rm = TRUE)) / 
    sd(out_ac$y.sd, na.rm = TRUE)
  out <- rbind(out, out_ac)
}

out
unique(data[,c('asset.code', 'yyqq')])
unique(out[,c('asset.code', 'yyqq')])

out$mat_mo <- (interval((out$issue.date), (out$matur.date)) %/% months(1))
out$mat_yr <- time_length(interval(out$issue.date, out$matur.date), 'years')
out$mat_yr_short <- round(out$mat_yr)
out <- out[out$type != 'NTN-B Principal',]

# now, save it so I can export it and join it to the other stuff. 
dim(data)
dir <- 'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet/brazil/'
fn <- paste0(dir, 'bond_prices_expanded.csv')
fn
# data <- read.csv(fn)
# head(data)
write.csv(out, fn)


# plot data (prices)
# first, I need the spread rather than the yield. 
# all the yields drop to zero when they're maturing. This means that the 
# thing I need to move because of the hhi is the yield at maturity, which is 
# just the short-term rate, pretty much. 
# this phenomenon is very obvious in the LTNs. All the prices converge to 1k at 
# ttm==0 and disperse after that. YTM converge to something under 0.2, which 
# must be the risk-free rate for that asset. 
# this means that the thing I think is going to respond to the hhi is the 
# volatility of the price, which should be the sd of the prices? check GT. 
# the volatility risk should be something more complicated. Is there a package
# for R that allows me to price volatility risk?
subset <- out[out$type == types[2], ]
scaleFactor <- max(subset$price.bid) / max(subset$yield.bid)
# ggplot(subset, aes(x=ref.date)) +
ggplot(subset, aes(x=ttm)) +
  geom_line(aes(y=price.bid, color = asset.code)) +
  # geom_line(aes(y=yield.bid, color = asset.code)) +
  geom_line(aes(y=yield.bid * scaleFactor, color = asset.code)) +
  scale_y_continuous(name="Price", 
                     sec.axis=sec_axis(~./scaleFactor, name="Yield")
                     ) +
  # xlim(0,500) +
  theme(
    axis.title.y.left=element_text(color="blue"),
    axis.text.y.left=element_text(color="blue"),
    axis.title.y.right=element_text(color="red"),
    axis.text.y.right=element_text(color="red")
  )


## try some basic regression. 
m0 <- lm(
          # p.sd ~
          price.bid ~
           # yield.bid ~
           # asset.code +
           type +
           ttm + 
           mat_yr +
           p.sd +
           y.sd
         # data = subset
         ,data = out)
summary(m0)

hhi_data <- read.csv(paste0(dir, 'brazil_reg_data.csv'),
                    stringsAsFactors = TRUE, 
                    row.names = 1,
                    header = TRUE)
head(hhi_data)
mh <- lm(
          # price.bid ~
          p.sd ~
            # y.sd ~ 
          # yield.bid ~
          # asset.code +
          # lag(price.bid) + 
          lag(yield.bid) + 
          # issue_px +
          issue_yd +
          # mat_px +
          mat_yd + 
          still_outstanding * ttm +
          still_outstanding * days_to_mty +
          hhi +
          type +
          mat_yr #+   
          # p.sd +
          # y.sd
        # data = subset
        ,data = hhi_data)
summary(mh)

library(lme4)

# fixed intercept for the non-changing terms. 
mh <- lmer(p.sd ~
             # y.sd + 
             mat_yr + 
             price.bid + 
             # lag(p.sd) +
             # issue_px +
             # issue_yd + 
             # mat_px + 
             # mat_yd +
             # still_outstanding * days_to_mty +
             hhi +
             still_outstanding * ttm + 
             # type + 
             (1 + mat_px + mat_yd
              | type )
          # data = subset
          ,data = hhi_data)
summary(mh)

rm(list=ls())
setwd("C:\\Users\\trmcdade\\OneDrive\\Laptop\\Duke\\RA\\Proposals\\Summer 2019 Proposal")

set.seed(0)
x <- rnorm(5000)

d <- density.default(x, n = 512, cut = 3)
d_fun <- ecdf (x)

#Assume a value for the "red vertical line"
e1 <- 3
e0 <- 1

#Area under curve between x0 and x1
auc1 <- round(d_fun(e1) - d_fun(e0), 2)

#Assume a value for the "red vertical line"
c1 <- 0.25
c0 <- -0.25

#Area under curve between x0 and x1
auc2 <- round(d_fun(c1) - d_fun(c0), 2)

png("pdf_example.png", width=6, height=5, units="in", family="serif", type="cairo", res=200)
par(mfrow=c(1,2))
plot(d, 
     main = paste0('Extreme Policy: AUC is ', auc1),
     xlab = 'Policy Space'); abline(v = e1, col = 2); abline(v = e0, col = 2)
plot(d,
     main = paste0('Centrist Policy: AUC is ', auc2),
     xlab = 'Policy Space'); abline(v = c1, col = 2); abline(v = c0, col = 2)
dev.off()

setwd("C:\\Users\\trmcdade\\OneDrive\\Laptop\\Duke\\RA\\Proposals\\Summer 2019 Proposal")
x = runif(500, 0, 10)
y = runif(500, 0, 10)

# a = (1+0.06) ^ x
# b = (1+0.05) ^ (x + (1/x))
# a = (1 + 0.06) #short term yield
# b = (1 + 0.05) ^ ((x + 1)/x) #long term yield

a <- (100/(100/((1 + 0.06) ^ x))) ^ (1/x) - 1
b <- (100/(100/((1 + 0.05) ^ x))) ^ (1/x) - 1
q = a/b

head(a)
head(b)
head(q)
q[q > 1]
a
# png("stgtlt.png", width=5, height=5, units="in", family="serif", type="cairo", res=200)
plot(x, q, col = 'blue', 
     xlim = c(0,10), 
     ylim = c(0,4),
     xlab = '(T-t), time remaining', 
     ylab = 'Value', 
     main = 'Inversion Point of The Yield Curve, r_st > r_lt')
points(x, b, col = 'red')
# points(x, q, col = 'green')
abline(h = 1, lty = 2)
# dev.off()

a = (1+0.05) ^ x
b = (1+0.05) ^ (x + (1/x))
q = a/b

png("stetlt.png", width=5, height=5, units="in", family="serif", type="cairo", res=200)
plot(x, a, col = 'blue', 
     xlim = c(0,10), 
     ylim = c(0,4), 
     xlab = '(T-t), time remaining', 
     ylab = 'Value', 
     main = 'Inversion Point of The Yield Curve, r_st = r_lt')
points(x, b, col = 'red')
points(x, q, col = 'green')
abline(h = 1, lty = 2)
dev.off()

a = (1+0.04) ^ x
b = (1+0.05) ^ (x + (1/x))
q = a/b

png("stltlt.png", width=5, height=5, units="in", family="serif", type="cairo", res=200)
plot(x, a, col = 'blue', 
     xlim = c(0,10), 
     ylim = c(0,4), 
     xlab = '(T-t), time remaining', 
     ylab = 'Value', 
     main = 'Inversion Point of The Yield Curve, r_st < r_lt')
points(x, b, col = 'red')
points(x, q, col = 'green')
abline(h = 1, lty = 2)
dev.off()
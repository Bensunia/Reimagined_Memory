#Installing and loading libraries
#install.packages("fBasics")
#install.packages("forecast")
library(fBasics)
library(forecast)

# Importing Homework Data
data<-read.csv("/Users/bensunia/Desktop/Forecasting\ Time\ Series/Assignment\ 1/Homework_1_DATA.csv", 
         header=TRUE, sep =";",dec=",")

#Checking the data set
head(data)
str(data)
names(data)



#Assigning Series
series1= data[,"Series.1"] [1:300]
series2= data[,"Series.2"] [1:300]
series3= data[,"Series.3"] [1:300]
series4= data[,"Series.4"] [1:300]
series5= data[,"Series.5"] [1:2000]
series6= data[,"Series.6"] 
series7= data[,"Series.7"] 

str(series7)

#Analyzing stationarity
y<-series7

par(mar=c(1,1,1,1)) # to adjust graphic size

par(mfrow=c(3,1)) # plot the series, its acf and pacf together
ts.plot(y)   
acf(y)
pacf(y)

#Computing basic statistics
mean(y) 
sd(y)
skewness(y)
kurtosis(y,method=c("moment"))  

# Augmented Dickey Fuller test. Testing for stationarity.
# Ho: the process is not stationary. We need, at least, a unit root
# H1: the process is stationary. We have to check different models (lags)

ndiffs(y, alpha=0.05, test=c("adf"))

#Checking for normality graphically
hist(y,prob=T,ylim=c(0,0.6),xlim=c(mean(y)-3*sd(y),mean(y)+3*sd(y)),col="red")
lines(density(y),lwd=2)
mu<-mean(y)
sigma<-sd(y)
x<-seq(mu-3*sigma,mu+3*sigma,length=100)
yy<-dnorm(x,mu,sigma)
lines(x,yy,lwd=2,col="blue")

# formal normality test
# Ho: the data is normally distributed
# H1: the data is not normally distributed
shapiro.test(y)

# Sometimes we will need to do the same for the transformed data "z"
# formal test for white noise (zero autocorrelations)
# Ho: uncorrelated data
# H1: correlated data
Box.test (y, lag = 20, type="Ljung")

# C.	Testing for STRICT WHITE NOISE

par(mar=c(1,1,1,1)) # to adjust graphic size

par(mfrow=c(3,1)) # analysis of the squared data
ts.plot(y^2)   
acf(y^2)
pacf(y^2)

# Sometimes we will need to do the same for the transformed data "z"
# formal test for white noise (zero autocorrelations)
# Ho: uncorrelated data
# H1: correlated data
Box.test(y^2,lag=20, type="Ljung")    # Null: ro1=.=ro20=0

# D.	Testing for GAUSSIAN WHITE NOISE
shapiro.test(y)
# GWN ??? SWN


####################################################################################
################ When transformation is needed use z ###############################
# Just in case we need to take one difference to the original data (as in this case)

z<-diff(y)
ts.plot(z)

par(mfrow=c(3,1))
ts.plot(z)   
acf(z)
pacf(z)

ndiffs(z, alpha=0.05, test=c("adf"))

mean(z)

# Ho: uncorrelated data
# H1: correlated data
Box.test (z, lag = 20, type="Ljung") 

Box.test (z^2, lag = 20, type="Ljung") 

#Checking for normality

shapiro.test(z)
# Ho: the data is normally distributed
# H1: the data is not normally distributed

hist(z,prob=T,ylim=c(0,0.6),xlim=c(mean(z)-3*sd(z),mean(z)+3*sd(z)),col="red")
lines(density(z),lwd=2)
mu<-mean(z)
sigma<-sd(z)
x<-seq(mu-3*sigma,mu+3*sigma,length=100)
yy<-dnorm(x,mu,sigma)
lines(x,yy,lwd=2,col="blue")


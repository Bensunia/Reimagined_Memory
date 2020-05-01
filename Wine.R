library(rgeos)
library(extrafont)
library(ggplot2)
library(ggthemes)
library(nlme)
library(gganimate)
library(gapminder)
library(ggExtra)
library(psych)
library(reshape2)
library(dplyr)
library(nycflights13)
library(ggcorrplot)
library(waffle)
library(tidyr)
library(scales)
library(ggalt)
library(data.table)
library(extrafont)
library(lubridate)
library(DT)
library(grid)
library(gridExtra)
library(prettydoc)
library(devtools)
library(tidyverse)
library(ggdark)
library(here)
library(png)
library(gifski)
library(forcats)
library(tufte)
library(colorspace)
library(viridisLite)
library(Zelig)
library(formatR)
library(DiagrammeR)
library(xaringan)
library(ggridges)
library(GGally)
library(maps)

########### Wine Reviews ############

###### 1. Import our data set (downloaded from Kaggle) #####     

wine<-read.csv('/Users/lina/Downloads/wine-reviews/winemag-data_first150k.csv')

wine<-wine[!is.na(wine$price),]
wine<-wine[!is.na(wine$points),]

##### 2. Variable Creation ####


#### 2.1 Column of ones and logarithm of the price ####

wine["one"]<-1 #to easily count by different categories

wine$ln_price = log(wine$price)


#### 2.2 Price Category ####

#we created a price_category variable based on the price of the wine. 
#as a reference, we used a simplified version of this segments by Wine Folly https://winefolly.com/update/reality-of-wine-prices-what-you-get-for-what-you-spend/

wine["price_category"]<-""

for (i in 1:length(wine$price_category)) {
  
  if(wine$price[i]<=20){
    
    wine$price_category[i] <- "cheap" }
  
  else if (wine$price[i] >15 & wine$price[i] <=50) {
    
    wine$price_category[i] <- "premium" 
    
  }
  
  else if (wine$price[i] >50 & wine$price[i] <=200) {
    
    wine$price_category[i] <- "luxury" 
    
  }
  
  else {
    
    wine$price_category[i] <- "iconic"
  }
  
}

table(wine$price_category)




###### 3. Exploratory Data Analysis #####

#3.1 Summary of Average Price, Points and Total Observations by Variety ####

wine_countries <- wine %>%
  group_by(country) %>%
  summarise( obs = sum(one), average_price = mean(price, na.rm=TRUE), average_points = mean(points, na.rm=TRUE))

wine_variety <- wine %>%
  group_by(variety) %>%
  summarise( obs = sum(one), average_price = mean(price, na.rm=TRUE), average_points = mean(points, na.rm=TRUE))

wine_winery <- wine %>%
  group_by(winery) %>%
  summarise( obs = sum(one), average_price = mean(price, na.rm=TRUE), average_points = mean(points, na.rm=TRUE))

wine_province <- wine %>%
  group_by(province) %>%
  summarise( obs = sum(one), average_price = mean(price, na.rm=TRUE), average_points = mean(points, na.rm=TRUE))


#as we have 49 different values for Countries and 632 values for wine Varieties, we are going to 
#filter our data base by the top 6 to do some specific analysis around these two metrics. 

top_6_countries<- wine %>%
  filter(country == 'US' | country == 'Italy' | country == 'France'| country == 'Spain'| country == 'Chile'| country == 'Argentina')

top_6_varieties<- wine %>%
  filter(variety == 'Chardonnay' | variety == 'Pinot Noir' | variety == 'Cabernet Sauvignon'| variety == 'Red Blend'| variety == 'Bordeaux-style Red Blend'| variety == 'Sauvignon Blanc')

top_6_countries_varieties <- top_6_countries %>%
  filter(variety == 'Chardonnay' | variety == 'Pinot Noir' | variety == 'Cabernet Sauvignon'| variety == 'Red Blend'| variety == 'Bordeaux-style Red Blend'| variety == 'Sauvignon Blanc')

###### 4.Creating my theme ########

fill_color = '#171616'
decoration_color = '#cccccc'
main1_color = "#C41E3D" #red
main2_color = "#8E0045" # purple
main3_color = "#8cb369"
main4_color = "#FDFFFC" #red
main5_color = "#F8FCDA"
main6_color = "#C45E70" #pink
main7_color = "#D4E251" #lime green


teamf_theme<-theme_bw() + theme(
  panel.grid.major = element_blank(), 
  panel.grid.minor = element_blank(),
  plot.title = element_text(size = 14, hjust = 0.2, color = decoration_color),
  axis.title = element_text(size = 10, hjust = 0.5, color = decoration_color),
  axis.text = element_text(colour = decoration_color, size = 15),
  axis.ticks = element_blank(),
  axis.line = element_line(colour = decoration_color, size=0.3, linetype = "dashed"), 
  panel.border = element_blank(),
  panel.grid = element_blank(),
  strip.text = element_text(size = 12, color = decoration_color),
  panel.background = element_blank(),
  strip.background =element_blank(),
  plot.background = element_blank(),
  legend.text	= element_text(size = 10, hjust = 0.5, color = decoration_color), 
  legend.position = c(0.815, 0.27),
  legend.key = element_blank(),
  legend.title = element_blank(),
  # labs.title = element_text(size = 14, hjust = 0.2, color = decoration_color)
)

theme_set(teamf_theme)

theme_set(dark_theme_gray()+ theme(
  panel.grid.major = element_blank(), 
  panel.grid.minor = element_blank(),
  plot.title = element_text(size = 14,color = decoration_color),
  axis.ticks = element_blank(),
  axis.text = element_text(colour = decoration_color, size = 10),
  axis.title = element_text(size = 10, color = decoration_color),
  legend.title = element_blank(),
  panel.background =element_rect(fill = fill_color),
  strip.background =element_rect(fill = fill_color), 
  plot.background = element_rect(fill = fill_color),
  legend.background = element_rect(fill = fill_color)
))
######### 5. Plots #########

#### 5.1 Points Distribution ####

ggplot(wine, aes(points)) + 
  geom_histogram(colour = main4_color, fill = main2_color, bins=21, size =0.1)+
  scale_x_continuous(breaks=seq(80,100,5))

#### 5.2 Price Distribution ####

#logarithm
ggplot(wine, aes(ln_price)) + 
  geom_histogram(colour = main4_color, fill = main2_color, size = 0.1)

#normal price
ggplot(wine, aes(price)) + 
  geom_histogram(colour = main4_color, fill = main2_color, size = 0.1)+

#3 zoom-ins
ggplot(wine, aes(price)) + 
  geom_histogram(colour = main2_color, fill = main1_color, binwidth = 0.50)+
  xlim(0, 100)
ggplot(wine, aes(price)) + 
  geom_histogram(colour = main2_color, fill = main1_color, binwidth = 0.50)+
  xlim(100, 500)
ggplot(wine, aes(price)) + 
  geom_histogram(colour = main2_color, fill = main1_color, binwidth = 0.50)+
  xlim(500, 2350)

#### 5.3 Countries distribution ####

wine_countries %>%
  filter(obs>2000)%>%
  arrange(obs) %>%
  ggplot(.,aes(x=reorder(country,-obs), y=obs, order=-obs) ) +
  geom_bar(stat='identity')+
  labs(y= "Observations", x = "Top Countries") 

#### 5.4 Varieties distribution ####

wine_variety %>%
  filter(obs>3000)%>%
  arrange(obs) %>%
  ggplot(.,aes(x=reorder(variety,-obs), y=obs, order=-obs) ) +
  geom_bar(stat='identity')+
  labs(y= "Observations", x = "Variety") %>%

#### 5.5 Ridge Plots

#### 5.5.1 Price -Variety

ggplot(top_6_varieties, aes(x = price, y = variety, fill=variety)) +
  geom_density_ridges(color=NA) +
  scale_fill_manual(values=c(main4_color, main1_color, main3_color, main2_color, main7_color, main6_color)) +
  xlim(0,200) +
  theme(legend.text=element_text(size=6), #making key text smaller
        legend.key.size = unit(0.5, "cm"), legend.position = "none")

#### 5.5.2 Price - Country

ggplot(top_6_countries, aes(x = price, y = country, fill=country)) +
  geom_density_ridges(color=NA) +
  scale_fill_manual(values=c(main4_color, main1_color, main3_color, main2_color, main7_color, main6_color)) +
  xlim(0,100) +
  theme(legend.text=element_text(size=6), #making key text smaller
        legend.key.size = unit(0.5, "cm"), legend.position = "none")

#### 5.6 Price-Points relationship ####

#### 5.6.1 Price-Points relationship by country ####
ggplot(top_6_countries, aes(x=points, y=ln_price)) +
  geom_point(color=main2_color, size=0.8, alpha=0.09)+
  facet_wrap(~ country) +
  stat_smooth(color=decoration_color,se = FALSE)

#### 5.6 Price-Points relationship by price_category ####
ggplot(top_6_varieties, aes(x=points, y=ln_price)) +
  geom_point(color=main2_color, size=0.8, alpha=0.09)+
  facet_wrap(~ pvariety) +
  stat_smooth(color=decoration_color,se = FALSE)



#### Just random stuff ####
#scatterplot color
ggplot(cheap_wines, aes(x=points, y=price)) + 
  geom_point()
  geom_point(size=0.2, alpha=0.12) +
  scale_colour_gradient(low = main1_color, high = main2_color) 

#####
ggplot(top_6_varieties, aes(x=points, y=ln_price,colour=variety)) + 
  geom_point(size=0.2, alpha=0.2) +
  scale_y_log10() +
  scale_x_log10() 

#### boxplot
ggplot(wine, aes(x=factor(points), y=ln_price)) + 
  geom_boxplot()



#log scale
ggplot(wine, aes(x=points, y=ln_price)) + 
  geom_point(size=0.1, alpha=0.09, color=main2_color) 
#boxplot log scale
ggplot(wine, aes(x=factor(points), y=ln_price)) + 
  geom_boxplot()

#Axis labeling depending on the quantiles 




ggplot(wine, aes(points)) + 
  geom_histogram(colour = main2_color, fill = main1_color, binwidth = 0.50) 

ggplot(top_6_varieties, aes(x=points, y=ln_price))+
  geom_point(color=main2_color, size=0.8, alpha=0.09)+
  facet_wrap( ~ variety, ncol=2, scales = "free") +
  stat_smooth(color=decoration_color)

######


ggplot(top_6_countries, aes(factor(country),ln_price)) + 
  geom_tufteboxplot(outlier.colour="transparent", size=0.5, color=main2_color)

wine_countries %>%
  filter(obs>2000)%>%
  arrange(obs) %>%
  ggplot(.,aes(x=reorder(country,-obs), y=obs, order=-obs) ) +
  geom_bar(stat='identity')

ggplot(corr.m, aes(x = reorder(miRNA, -value), y = value, fill = variable)) + 
  geom_bar(stat = "identity")



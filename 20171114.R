library(ggplot2)
library(lubridate)

library(zoo)
library(stringr)
library(plyr)
library(dplyr)
library(doBy)
library(haven)
library(data.table)

#setwd("C:/Users/deko/Downloads/claims")
setwd("C:/r_projects/triangles_daten/input")

mydata<- read.table("OutSt.txt", header=FALSE, sep=";")
mypayments<- read.table("Payments.txt", header=FALSE,sep=";")
df<-data.frame(mydata)
dfpayments<-data.frame(mypayments)

#### renaming columns-reserve data
colnames(df)[1] <- "ID"
colnames(df)[2] <- "Claim_Number"
colnames(df)[3] <- "Claim_Registration_Date"
colnames(df)[4] <- "Claim_Date"
colnames(df)[5] <- "Transaction_Order"
colnames(df)[6] <- "Is_Material"
colnames(df)[7] <- "Transaction_Date"
colnames(df)[8] <- "Reserve"
colnames(df)[9] <- "Project"

##### renaming columns- payment data
colnames(dfpayments)[1] <- "ID"
colnames(dfpayments)[2] <- "Claim_Number"
colnames(dfpayments)[3] <- "Claim_Registration_Date"
colnames(dfpayments)[4] <- "Claim_Date"
colnames(dfpayments)[5] <- "Is_Material"
colnames(dfpayments)[6] <- "Transaction_Date"
colnames(dfpayments)[7] <- "Payments"
colnames(dfpayments)[8] <- "Project"

#setting the datess as format datas
df$Claim_Registration_Date<-as.Date(df$Claim_Registration_Date, "%d/%m/%Y")
df$Claim_Date<-as.Date(df$Claim_Date, "%d/%m/%Y")
df$Transaction_Date<-as.Date(df$Transaction_Date, "%d/%m/%Y")
dfpayments$Claim_Registration_Date<-as.Date(dfpayments$Claim_Registration_Date, "%d/%m/%Y")
dfpayments$Claim_Date<-as.Date(dfpayments$Claim_Date, "%d/%m/%Y")
dfpayments$Transaction_Date<-as.Date(dfpayments$Transaction_Date, "%d/%m/%Y")
k<-subset(dfpayments,dfpayments$Claim_Number=="00410240045")


df$Claim_Registration_Date<-as.Date(df$Claim_Registration_Date, "%d/%m/%Y")
df$Claim_Date<-as.Date(df$Claim_Date, "%d/%m/%Y")
df$Transaction_Date<-as.Date(df$Transaction_Date, "%d/%m/%Y")

payments<-data.frame("payments")
reserve<-data.frame("Reserve")
transactionorder<-data.frame("Transaction_Order")

df<-cbind(df,payments)
dfpayments<-cbind(dfpayments,reserve,transactionorder)

colnames(df)[10]<-"Payments"
colnames(dfpayments)[9]<-"Reserve"

colnames(dfpayments)[10]<-"Transaction_Order"
df$Payments<-NA
dfpayments$Reserve<-NA

dfpayments$Transaction_Order<-NA

alldata<-rbind(df,dfpayments)
any(is.na(alldata$Transaction_Date))
wrong<-subset(alldata, as.Date(format(Transaction_Date,"%Y/%m/%d" ))<as.Date("1990/01/01"))
alldata<-alldata %>%
  group_by(Claim_Number,Is_Material) %>%
  mutate(Transaction_Date=ifelse(Transaction_Date<"1990/01/01", Claim_Registration_Date,Transaction_Date  ))
alldata$Transaction_Date<-as.Date(alldata$Transaction_Date)
#####set together tkhe two dataframes


#####Setting all registration dates to the min if duplicates
alldata<-alldata %>%
  group_by(Claim_Number,Is_Material) %>%
  mutate(max = if_else(Claim_Registration_Date == min(Claim_Registration_Date), Claim_Registration_Date, Claim_Registration_Date)) 
alldata$Claim_Registration_Date<-NULL
colnames(alldata)[10]<-"Claim_Registration_Date"

kall1<-subset(alldata,alldata$Claim_Number=="00410240045")
wrong<-subset(alldata, as.Date(format(Transaction_Date,"%Y/%m/%d" ))<"01/01/1990")

####all claim dates as min
alldata<-alldata %>%
  group_by(Claim_Number, Is_Material) %>%
  mutate(max = if_else(Claim_Date == min(Claim_Date), Claim_Date, Claim_Date)) 
alldata$Claim_Date<-NULL
colnames(alldata)[10]<-"Claim_Date"

kall2<-subset(alldata,alldata$Claim_Number=="053K0010040")

#### Correction reserve dates

#### Correction reserve dates
RegClaimR<-subset(alldata, as.Date(format(Claim_Registration_Date,"%Y/%m/%d" ))>as.Date(format(Claim_Date,"%Y/%m/%d" )))
alldata <- transform(alldata, Claim_Registration_Date = pmin(Claim_Registration_Date, Claim_Date))


RegTransactionR<-subset(alldata, as.Date(format(Claim_Registration_Date,"%Y/%m/%d" ))>as.Date(format(Transaction_Date,"%Y/%m/%d" )))
alldata <- transform(alldata, Claim_Registration_Date= pmin(Claim_Registration_Date, Transaction_Date))

TransClaimR<-subset(alldata, as.Date(format(Transaction_Date,"%Y/%m/%d" ))<as.Date(format(Claim_Date,"%Y/%m/%d" )))
alldata<- transform(alldata, Claim_Date=pmin(Transaction_Date, Claim_Date))
RegClaimR<-subset(alldata, as.Date(format(Claim_Registration_Date,"%Y/%m/%d" ))>as.Date(format(Claim_Date,"%Y/%m/%d" )))
alldata <- transform(alldata, Claim_Registration_Date = pmin(Claim_Registration_Date, Claim_Date))


kall3<-subset(alldata,alldata$Claim_Number=="00410240045")



####splitting to two data frames
dfpayments<-subset(alldata, is.na(Transaction_Order))
df<-subset(alldata,is.na(Payments))
df$Payments<-NULL
dfpayments$Transaction_Order<-NULL
dfpayments$Reserve<-NULL
k<-subset(alldata,alldata$Claim_Number==99470550001)
kall5<-subset(alldata,alldata$Claim_Number=="053K0010040")

#export alldata for testing purpose
write.table(alldata, "C:/r_projects/triangles_daten/temp_testing/alldata.txt", sep="\t")
write.table(df, "C:/r_projects/triangles_daten/temp_testing/reserve.txt", sep="\t")
write.table(dfpayments, "C:/r_projects/triangles_daten/temp_testing/payments.txt", sep="\t")
payments_SAS <- read_sas("C:/r_projects/triangles_daten/output/payments.sas7bdat")
reserve_SAS <- read_sas("C:/r_projects/triangles_daten/output/reserve.sas7bdat")
write.table(payments_SAS, "C:/r_projects/triangles_daten/temp_testing/payments_SAS.txt", sep="\t")
write.table(reserve_SAS, "C:/r_projects/triangles_daten/temp_testing/reserve_SAS.txt", sep="\t")


####splitting the 3&4 position of the Claim_Number
x<- df$Claim_Number
insurance_type<-substring(x, 3,4)
insurance_t<-data.frame(insurance_type)
df$insurance<- insurance_type

y<- dfpayments$Claim_Number
insurance_typem<-substring(y, 3,4)
insurance_tm<-data.frame(insurance_typem)
dfpayments$insurance<- insurance_typem

#####
type<- data.frame(matrix(ncol = 1, nrow = 374738))
df$type<-type
typem<- data.frame(matrix(ncol = 1, nrow=179481))
dfpayments$type<-typem
####material and nonmaterial claims
material<- subset(df, df$Is_Material==1)
nonmaterial<-subset(df,Is_Material==0)

######defining the insurance type for the reserve

df$type<-if_else(df$insurance==11|df$insurance==16 | df$insurance==19 |df$insurance==15| df$insurance==62,"Motohull",
                 ifelse(df$insurance==64|df$insurance==65|substring(df$insurance,1,1)=="E", "Technic",
                        ifelse(df$insurance==42,"ActionTPL",
                               ifelse(df$insurance==12|df$insurance==61, "AviationHull",
                                      ifelse(df$insurance=="3K", "Assistance",
                                             ifelse(substring(df$insurance,1,1)==0|df$insurance==10|df$insurance==13|df$insurance==14|df$insurance==16|df$insurance==40|df$insurance=="4T"|df$insurance=="L6","Marine",
                                                    ifelse(substring(df$insurance,1,1)==2|substring(df$insurance,1,1)==8|df$insurance=="BI"|df$insurance=="BA"|df$insurance==63|df$insurance==66|df$insurance=="Q1"|df$insurance=="FR"|df$insurance=="F1","other",   
                                                           
                                                           
                                                           ifelse(substring(df$insurance,1,1)==7,"Animal",
                                                                  ifelse(df$insurance==43|df$insurance==44|df$insurance==45|df$insurance==48||df$insurance==49|df$insurance=="4C"|df$insurance=="4D"|df$insurance=="4E"|df$insurance=="4H"|df$insurance=="4L"|df$insurance=="4M"|df$insurance=="4P"|df$insurance=="4R"|df$insurance==67|df$insurance=="F2"|df$insurance=="Q2"|substring(df$insurance,1,1)=="L"|substring(df$insurance,1,1)=="P", "Liability",
                                                                         ifelse(df$insurance=="6H"|substring(df$insurance,1,1)==3,"Accident",
                                                                                ifelse(substring(df$insurance,1,1)==9,"Bonds",
                                                                                       ifelse(substring(df$insurance,1,1)=="K"|substring(df$insurance,1,1)=="H","Credit",
                                                                                              ifelse(df$insurance==41|df$insurance==46|df$insurance==47|df$insurance=="4B"|df$insurance=="4G"|df$insurance=="4Z"||df$insurance=="4U"|df$insurance=="4A"&df$Is_Material==1,"MTPL_Material","MTPL_nonmaterial")))))  ))))))))


#####separating to material and non material insurance type
#df$type[df$type=="GreenCard"&df$Is_Material==1]<-"GreenCard_material"
#df$type[df$type=="GreenCard"&df$Is_Material==0]<-"GreenCard_nonmaterial"
#df$type[df$type=="Liability"&df$Is_Material==1]<-"Liability_material"
#df$type[df$type=="Liability"&df$Is_Material==0]<-"Liability_nonmaterial"
#df$type[df$type=="MTPL"&df$Is_Material==1]<-"MTPL_material"
#df$type[df$type=="MTPL"&df$Is_Material==0]<-"MTPL_nonmaterial"
#df$type[df$type=="Animal"&df$Is_Material==1]<-"Animal_material"
#df$type[df$type=="Animal"&df$Is_Material==0]<-"Animal_nonmaterial"
#df$type[df$type=="GreenCard"&df$Is_Material==1]<-"GreenCard_material"
#df$type[df$type=="GreenCard"&df$Is_Material==0]<-"GreenCard_nonmaterial"
#df$type[df$type=="Accident"&df$Is_Material==1]<-"Accident_material"
#df$type[df$type=="Accident"&df$Is_Material==0]<-"Accident_nonmaterial"

###### defining the type of insurance for the payment set
dfpayments$type<-ifelse(dfpayments$insurance==11|dfpayments$insurance==16 | dfpayments$insurance==19 | dfpayments$insurance==62,"Motohull",
                        ifelse(dfpayments$insurance==64|dfpayments$insurance==65|substring(dfpayments$insurance,1,1)=="E", "Technic",
                               ifelse(dfpayments$insurance==42,"ActionTPL",
                                      ifelse(dfpayments$insurance==12|dfpayments$insurance==61, "AviationHull",
                                             ifelse(dfpayments$insurance=="3K", "Assistance",
                                                    ifelse(substring(dfpayments$insurance,1,1)==0|dfpayments$insurance==10|dfpayments$insurance==13|dfpayments$insurance==14|dfpayments$insurance==16|dfpayments$insurance==40|dfpayments$insurance=="4T"|dfpayments$insurance=="L6","Marine",
                                                           
                                                           ifelse(substring(dfpayments$insurance,1,1)==2|substring(dfpayments$insurance,1,1)==8|dfpayments$insurance=="BI"|dfpayments$insurance=="BA"|dfpayments$insurance==63|dfpayments$insurance==66|dfpayments$insurance=="Q1"|dfpayments$insurance=="FR"|dfpayments$insurance=="F1","other",    
                                                                  ifelse(substring(dfpayments$insurance,1,1)==7,"Animal",
                                                                         ifelse(dfpayments$insurance==43|dfpayments$insurance==44|dfpayments$insurance==45|dfpayments$insurance==48||dfpayments$insurance==49|dfpayments$insurance=="4C"|dfpayments$insurance=="4D"|dfpayments$insurance=="4E"|dfpayments$insurance=="4H"|dfpayments$insurance=="4L"|dfpayments$insurance=="4M"|dfpayments$insurance=="4P"|dfpayments$insurance=="4R"|dfpayments$insurance==67|dfpayments$insurance=="F2"|dfpayments$insurance=="Q2"|substring(dfpayments$insurance,1,1)=="L"|substring(dfpayments$insurance,1,1)=="P", "Liability",
                                                                                ifelse(dfpayments$insurance=="6H"|substring(dfpayments$insurance,1,1)==3,"Accident",
                                                                                       ifelse(substring(dfpayments$insurance,1,1)==9,"Bonds",
                                                                                              ifelse(substring(dfpayments$insurance,1,1)=="K"|substring(dfpayments$insurance,1,1)=="H","Credit",
                                                                                                     ifelse(dfpayments$insurance==41|dfpayments$insurance==46|dfpayments$insurance==47|dfpayments$insurance=="4B"|dfpayments$insurance=="4G"|dfpayments$insurance=="4Z"||dfpayments$insurance=="4U"|dfpayments$insurance=="4A"&dfpayments$Is_Material==1,"MTPL_Material","MTPL_nonmaterial"))))  )))))))))


#####separating to material and non material insurance type
#dfpayments$type[dfpayments$type=="GreenCard"&dfpayments$Is_Material==1]<-"GreenCard_material"
#dfpayments$type[dfpayments$type=="GreenCard"&dfpayments$Is_Material==0]<-"GreenCard_nonmaterial"
#dfpayments$type[dfpayments$type=="Liability"&dfpayments$Is_Material==1]<-"Liability_material"
#dfpayments$type[dfpayments$type=="Liability"&dfpayments$Is_Material==0]<-"Liability_nonmaterial"
#dfpayments$type[dfpayments$type=="MTPL"&dfpayments$Is_Material==1]<-"MTPL_material"
#dfpayments$type[dfpayments$type=="MTPL"&dfpayments$Is_Material==0]<-"MTPL_nonmaterial"
#dfpayments$type[dfpayments$type=="Animal"&dfpayments$Is_Material==1]<-"Animal_material"
#dfpayments$type[dfpayments$type=="Animal"&dfpayments$Is_Material==0]<-"Animal_nonmaterial"
#dfpayments$type[dfpayments$type=="GreenCard"&dfpayments$Is_Material==1]<-"GreenCard_material"
#dfpayments$type[dfpayments$type=="GreenCard"&dfpayments$Is_Material==0]<-"GreenCard_nonmaterial"
#dfpayments$type[dfpayments$type=="Accident"&dfpayments$Is_Material==1]<-"Accident_material"
#dfpayments$type[dfpayments$type=="Accident"&dfpayments$Is_Material==0]<-"Accident_nonmaterial"



#za popylwane na praznite trimese4iq po rezerwite
df<- orderBy(~Claim_Number+Transaction_Date, data=df)
dm<-aggregate(df$Transaction_Date,by=list(df$Claim_Number),max)
dp<-aggregate(df$Transaction_Date,by=list(df$Claim_Number),min)
dm<-merge(dm,dp,by.x="Group.1", by.y="Group.1")
dm$x.x<-as.Date(dm$x.x, "%d/%m/%Y")
dm$x.y<-as.Date(dm$x.y, "%d/%m/%Y")

dm$x.x<-as.Date("06/06/2009", "%d/%m/%Y")




#trimese4ie + LIPSVA6TITE TRIMESE4IA
p<-setDT(dm)[,.(date = seq(as.Date(x.y), as.Date(x.x), by = "3 months")) ,.(Group.1)]
p$date<-as.yearqtr(p$date, format = "%d/%m/%Y")
dm$x.x<-as.yearqtr(dm$x.x, format = "%d/%m/%Y")
pm<-subset(dm,select= c(1,2))
pm<-as.data.frame(pm)
colnames(pm)[2]<-"date"
p<-rbind(p,pm )


library(data.table) 




##### summing payments for the same claims
#payment_total_individualclaim<-aggregate(dfpayments$Payments~dfpayments$Claim_Number,data=dfpayments,FUN=sum)

#konvertira datite kym trimese4ie
dfpayments$Claim_Registration_Date <- as.yearqtr(dfpayments$Claim_Registration_Date, format = "%d/%m/%Y")
dfpayments$Transaction_Date <- as.yearqtr(dfpayments$Transaction_Date, format = "%d/%m/%Y")
dfpayments$Claim_Date <- as.yearqtr(dfpayments$Claim_Date, format = "%d/%m/%Y")

df$Transaction_Date <- as.yearqtr(df$Transaction_Date, format = "%d/%m/%Y")
df$Claim_Registration_Date <- as.yearqtr(df$Claim_Registration_Date, format = "%d/%m/%Y")
df$Claim_Date <- as.yearqtr(df$Claim_Date, format = "%d/%m/%Y")


### adding count column

indx<-rep(1,length(dfpayments$Claim_Date))
indx<- data.frame(indx)
dfpayments<-cbind(indx, dfpayments)


list<-aggregate(cbind(dfpayments$Payments,dfpayments$indx)~dfpayments$Transaction_Date+dfpayments$Claim_Date+dfpayments$type, data=dfpayments, FUN=sum)
colnames(list)[3]<-"type"
colnames(list)[2]<-"Claim_Date"


openclaims<-aggregate(df$Reserve, by=list(df$Claim_Date, df$Claim_Number,df$Transaction_Date,df$type), FUN=head,1)
openclaims$index<-1
colnames(openclaims)[1]<-"Claim_Date"
colnames(openclaims)[2]<-"Claim_Number"
colnames(openclaims)[3]<-"Transaction_Date"
colnames(openclaims)[4]<-"Type"
colnames(openclaims)[5]<-"Reserve"
openclaimss<-aggregate(openclaims$index, by=list(openclaims$Claim_Date,openclaims$Transaction_Date,openclaims$Type), FUN=sum)

colnames(openclaimss)[1]<-"Claim_Date"
colnames(openclaimss)[2]<-"Transaction_Date"
colnames(openclaimss)[3]<-"Type"
colnames(openclaimss)[4]<-"Numberofopenclaims"



reserve<-aggregate(cbind(df$Transaction_Order,df$Reserve),by=list(df$Claim_Number,df$Claim_Date,df$Transaction_Date,df$type),FUN=tail, 1)

reserve<- orderBy(~Group.1+Group.4+V1, data=reserve)
nug<-subset(reserve, reserve$Group.3=="01/01"&reserve$Group.5=="Motohub"&reserve$Group.4=="01/01"&reserve$indx2==1)
indx2<-rep(1,length((reserve$Group.1)))
indx2<-data.frame(indx2)
reserve<-cbind(reserve,indx2)
reserve$indx2[reserve$V2==0]<-0
reserve$indx2[reserve$V2!=0]<-1
colnames(p)[1]<-"Claim_Number"
colnames(p)[2]<-"Transaction_Date"
colnames(reserve)[1]<-"Claim_Number"
colnames(reserve)[3]<-"Transaction_Date"
reserve2<-merge(p,reserve, by.x=c("Claim_Number","Transaction_Date"),  by.y=c("Claim_Number","Transaction_Date"),all.x=TRUE)
reserve<- data.frame(reserve)


library(zoo)         # for na.locf(...)
library(data.table)
####adding the missing reserves

setDT(reserve2)[,Group.2:=na.locf(Group.2, na.rm=FALSE),by=Claim_Number]
reserve2[,Group.2:=if (is.na(Group.2[1])) c(mean(Group.2,na.rm=TRUE),variable[-1]) else Group.2,by=Claim_Number]

setDT(reserve2)[,Group.4:=na.locf(Group.4, na.rm=FALSE),by=Claim_Number]
reserve2[,Group.4:=if (is.na(Group.4[1])) c(mean(Group.4,na.rm=TRUE),variable[-1]) else Group.4,by=Claim_Number]
setDT(reserve2)[,Group.5:=na.locf(Group.5, na.rm=FALSE),by=Claim_Number]
reserve2[,Group.5:=if (is.na(Group.5[1])) c(mean(Group.5,na.rm=TRUE),variable[-1]) else Group.5,by=Claim_Number]
setDT(reserve2)[,V1:=na.locf(V1, na.rm=FALSE),by=Claim_Number]
reserve2[,V1:=if (is.na(V1[1])) c(mean(V1,na.rm=TRUE),variable[-1]) else V1,by=Claim_Number]
setDT(reserve2)[,V2:=na.locf(V2, na.rm=FALSE),by=Claim_Number]
reserve2[,V2:=if (is.na(V2[1])) c(mean(V2,na.rm=TRUE),variable[-1]) else V2,by=Claim_Number]
reserve2$indx2[reserve2$V2==0]<-0
reserve2$indx2[reserve2$V2!=0]<-1

reserve2<-unique(reserve2, by = c("Claim_Number","Group.4","Group.2", "Transaction_Date","V1","V2","indx2"))



#preobrazuwa i sumira na niwo trimese4ie
rese<-aggregate(cbind(reserve2$V2, reserve2$indx2),by=list(reserve2$Group.2,reserve2$Transaction_Date,reserve2$Group.4), FUN=sum)

#taking subset of reserve with type Motohub
moto<-subset(rese, rese$Group.3=="Motohub")
moto<-orderBy(~Group.1, data=moto)
###final reserve dataset
rese<-orderBy(~ Group.3, data=rese)
colnames(rese)[1] <- "Claim_Date"
colnames(rese)[2] <- "Transaction_Date"
colnames(rese)[3] <- "Type"
colnames(rese)[5] <- "NumberofClaims"
colnames(rese)[4] <- "Reserve"
####renaming list columns
colnames(list)[1] <- "Transaction_Date"

colnames(list)[3] <- "Type"
colnames(list)[4] <- "Payments"
colnames(list)[5]<-"Numberofclaims"
rese<-data.frame(rese)
list<-data.frame(list)
#reser=data.frame(rese)
mtpl<-subset(rese, rese$Type=="MTPL_Material"|rese$Type=="MTPL_nonmaterial")
mtplall<-aggregate(cbind(mtpl$Reserve,mtpl$NumberofClaims), by=list(mtpl$Claim_Date,mtpl$Transaction_Date), FUN=sum)
mtplall<-data.frame(mtplall)
mtplall$Type<-"MTPL"
colnames(mtplall)[1]<-"Claim_Date"
colnames(mtplall)[2]<-"Transaction_Date"
colnames(mtplall)[3]<-"Reserve"
colnames(mtplall)[4]<-"NumberofClaims"
rese<-rbind(rese,mtplall)
mtp<-subset(list, list$Type=="MTPL_Material"|list$Type=="MTPL_nonmaterial")
mtpall<-aggregate(cbind(mtp$Payments,mtp$Numberofclaims), by=list(mtp$Claim_Date,mtp$Transaction_Date), FUN=sum)
mtpall<-data.frame(mtpall)
mtpall$Type<-"MTPL"

colnames(mtpall)[1]<-"Claim_Date"
colnames(mtpall)[2]<-"Transaction_Date"
colnames(mtpall)[3]<-"Payments"
colnames(mtpall)[4]<-"Numberofclaims"
list<-rbind(mtpall,list)
####renaming reserve columns
k<-subset(openclaims, openclaims$Claim_Number=="97410070005")

colnames(rese)[1] <- "Claim_Date"
colnames(rese)[2] <- "Transaction_Date"
colnames(rese)[3] <- "Type"
colnames(rese)[5] <- "NumberofClaims"
colnames(rese)[4] <- "Reserve"
####renaming list columns


total<-merge(rese,list, by=c( "Claim_Date","Transaction_Date","Type"), all=TRUE)
total1<-merge(total, openclaimss, by=c( "Claim_Date","Transaction_Date","Type"), all=TRUE)



write.table(total, "C:/r_projects/triangles_daten/temp_testing/ccc.txt", sep="\t")

write.table(total1, "C:/r_projects/triangles_daten/temp_testing/total1.txt", sep="\t")

motohull<-subset(total, total$Type=="Motohull")
write.table(motohull, "C:/r_projects/triangles_daten/temp_testing/motohull.txt", sep="\t")

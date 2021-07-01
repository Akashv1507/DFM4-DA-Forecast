import pandas as pd 
import datetime as dt
import joblib
import numpy as np
import itertools
import tensorflow as tf
from src.utils.getSortedStationList import getSortedStationList


class MlpPredictions():
    """MLp prediction class
    """
    
    def __init__(self, modelPath: str) -> None:
        """load prediction model path
        Args:
            modelPath ([type]): path of model
        """
        self.modelPath = modelPath
        self.modelPathStr =""

    def stationCombinationFinder(self, tempData, sortedStationList):
        
        #Reordering temperature dataframe
        reorderedTempData = tempData.reindex(columns = sortedStationList)
        #Creating combination for all the weather station temperature series (Simple Average)
        combinationDf = pd.DataFrame()
        for i in range(len(reorderedTempData.columns)):
            combinationDf["C" + str(i+1)] = reorderedTempData.iloc[:,0:i+1].mean(axis=1)
        return combinationDf
    
    
    def featureDfgenerator(self, demandData, combinationSet,
                       weatherCombinationCode, holidayDf):
       
        rangeList = [np.arange(1,97) for i in range(int(len(demandData)/96))]
        timeblockList = itertools.chain.from_iterable(rangeList)
        CalenderDF = pd.DataFrame(index= demandData.index)
        CalenderDF['MONTH'] = demandData.index.month 
        CalenderDF['DAY'] = demandData.index.day 
        CalenderDF['HOUR'] = demandData.index.hour
        CalenderDF["TIMEBLOCK"] = list(timeblockList)
        CalenderDF['DAY-OF-WEEK'] = demandData.index.weekday
        CalenderDF["IS_WEEKEND"] = np.where((CalenderDF["DAY-OF-WEEK"] < 6), 0, 1)
        CalenderDF["HOLIDAY"] = np.isin(CalenderDF.index.date, holidayDf.index.date).astype("int")
        CalenderDF["WEATHER"] = combinationSet["{}".format(weatherCombinationCode)]
        featureDF = pd.concat([CalenderDF, demandData], axis= 1)   
        return featureDF
          

    def predictDaMlp(self, lagDemandDf:pd.core.frame.DataFrame, weatherDf:pd.core.frame.DataFrame, holidayDf:pd.core.frame.DataFrame, entityTag:str, virtualWeatherStsnNo:str)-> pd.core.frame.DataFrame:
        """predict DA forecast using model based on entity

        Args:
            lagDemandDf (pd.core.frame.DataFrame): dataframe containing blockwise D-2, D-7, D-14, D-21 demand with index timestamp of 'D'
            weatherDf(pd.core.frame.DataFrame): Weather data of forecasted date
            holidayDf(pd.core.frame.DataFrame) : holiday dummies in df
            entityTag (str): entity tag like 'WRLDCMP.SCADA1.A0047000'
            virtualWeatherStsnNo(str): virtual weather stsn number of particular entity say 'C6'

        Returns:
            pd.core.frame.DataFrame: DA demand forecast with column(timestamp, entityTag, demandValue)
        """    

        #setting model path string(class variable) based on entity tag(means deciding which model ti use)
        self.modelPathStr = self.modelPath + '\\' + str(entityTag) +'.h5'

        #Loading the model
        model_h5 = tf.keras.models.load_model(self.modelPathStr)

        # getting sorted station list of particular entity
        sortedStationList = getSortedStationList(entityTag=entityTag)

        combinationDf =self.stationCombinationFinder(weatherDf, sortedStationList= sortedStationList)

        featureDf = self.featureDfgenerator(lagDemandDf, combinationDf, virtualWeatherStsnNo, holidayDf)
        featureDfArr = featureDf.values.reshape(1,featureDf.shape[0], featureDf.shape[1])
        Yhat = model_h5.predict(featureDfArr, verbose= 0)

        daPredictionDf = pd.DataFrame(Yhat[0], index= featureDf.index)
        #renaming columns
        daPredictionDf.rename(columns={ 0: 'forecastedDemand'}, inplace=True)
        #adding entityTag column and resetting index
        daPredictionDf.insert(0, "entityTag", entityTag)  
        daPredictionDf.reset_index(inplace=True)
        return daPredictionDf
    
        


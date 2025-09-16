
# **NFL Data Analysis & Predictions**

# Prediction Module

## What are we building? 
We will build a predictive analytics module that leverages machine learning to predict: 
- ***Game outcomes***
- ***Game scores***
- ***Player key stats predictions***

## Where will we get the data? 
For NFL data, there are a few core data sources we can leverage: 
- ***nfl_data_py***: an API with rich game & player data, including AWS next-gen stats
- ***ESPN API***: a direct connection to ESPN for key info like schedules, prior records, etc. 
- ***The Odds API***: an API for odds / data from sportsbooks on upcoming events. 

## What are the outcomes we're looking to get to? 
- The predictive analytics module can help users who are looking to bet on their favorite sports, giving insights into which players & teams are expected to outperform in a given week. 
- This space will also be a go-to for Fantasy Sports players to help plan their team for the week. They should be able to identify which players are expected to outperform in the given week, so that they can optimize their starting roster accordingly. 

## User Stories & Needs
- Kaia is a 24 y/o NFL fan, who enjoys watching the games with her friends and has started to explore sports betting to add to the excitement of the games. She's been following others on X / social platforms, but wants to get some more detailed insights & predictions to help decide where to put her money down. 
- Jillian is a 32 y/o who is in a Fantasy Football league with her colleagues. She's a passive fan, but is interested in the competitive side of things when it comes to Fantasy, but she needs some help deciding who to start at each position ahead of the coming week. 

## Important components & considerations

**Accuracy & Transparency Module** 
Within our UI, we should have a section that covers the following: 
- Model performance and accuracy metrics for all predictive algos
- After each event, there should be an analysis of predicted outcomes vs. actual outcomes. 

# Analytics Module

## What are we building? 
We will build robust reporting & analytics on team and player performance. Want to see a high level summary of your team's last game (or season to date) performance? Or, where standings sit today? The core analytics module will support this. This should be tightly coupled with the prediction module, as it's important that the prediction module also includes historical performance and analytics. 

## Where will we get the data? 
Should be the same sources as prediction module. 

## What are the outcomes we're looking to get to? 
- ***Team Summary*** - win/loss, passing yards, rushing yards, penalties against on offense / defense, 3rd down conversion, etc. 
- ***Player Summary*** - top passers, top rushers, top QBs by QB rating, etc. 

## Tech Stack
- Dagster
- dbt 
- GCP | BigQuery, Cloud Scheduler
- scikit-learn, tensorflow, or pytorch for ML dev
- ?? for Data Viz / BI / Interface Layer




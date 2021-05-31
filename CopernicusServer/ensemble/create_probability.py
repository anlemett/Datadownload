import pandas as pd

year = 2020

month = 7

#airport = "Kiruna"
airport = "Malmo"
#airport = "Ovik"
#airport = "Sundsvall"
#airport = "Umeo"

#metric = 'wind'
#threshold = 4
#threshold = 7.716667 # 15 knots
#threshold = 12.86111 # 25 knots
#threshold = 18.00556 # 35 knots

metric = 'i10fg'
#threshold = 7.716667 # 15 knots
#threshold = 12.86111 # 25 knots
threshold = 18.00556 # 35 knots

#metric = 'tp'
#threshold = 0.001
#threshold = 0.002

#metric = 'sf'
#threshold = 0.001
#threshold = 0.002


#metric = 'cbh'
#threshold = 61
#threshold = 500

#metric = 'lcc'
#threshold = 0.95
#threshold = 1
#threshold = 0.8

#metric = 'cape'
#threshold = 150

#metric = 'cp'
#threshold = 0.00025





input_csv_filename = 'data/' + airport + '/' + airport + '_' + metric + '_' + str(year) + '_' + str(month) + '_ensemble.csv'
output_csv_filename = 'data/' + airport + '/probability_' + airport + '_' + str(year) + '_' + str(month) + '_' + metric + '_' + str(threshold) +'.csv'



def calculateProbabilityAboveThreshold(row):
    sum = 0
    for i in range(4,14):
        if row[i] >= threshold:
            sum = sum + 1
    return sum/10


def calculateProbabilityBelowThreshold(row):
    sum = 0
    for i in range(4,14):
        if row[i] <= threshold:
            sum = sum + 1
    return sum/10


df = pd.read_csv(input_csv_filename, sep=' ')


if metric == "cbh":
    df['probability'] = df.apply(lambda row: calculateProbabilityBelowThreshold(row), axis=1)
else:
    df['probability'] = df.apply(lambda row: calculateProbabilityAboveThreshold(row), axis=1)

df = df[['date', 'hour', 'probability']]
df['date'] = df['date'].astype(int)


cols = ['date', 'hour', 'probability']
lst = []

for index, row in df.iterrows():

    previous_hour = row['hour']-1 if row['hour']>0 else 23
    date = row['date']  if row['hour']>0 else row['date']-1
    lst.append([date, previous_hour, row['probability']])
    
    lst.append([row['date'], row['hour'], row['probability']])
    
    next_hour = row['hour']+1
    lst.append([row['date'], next_hour, row['probability']])

new_df = pd.DataFrame(lst, columns=cols)

# remove the first row
new_df = new_df.drop(new_df.index[0])

# copy the last row
new_data = pd.DataFrame(new_df[-1:].values, columns=new_df.columns)
new_data['hour'] = 23
new_df = new_df.append(new_data)


new_df.to_csv(output_csv_filename, sep=' ', float_format='%.6f', encoding='utf-8', header=True, index=False)


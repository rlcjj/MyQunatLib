import numpy as np
import pandas as pd


a= [[1,2,3],[2,3,5],[2,4,5],[2,3,4],[5,5,7]]
dt = np.array(a)

df = pd.DataFrame(dt,index=['1','2','3','4','5'],columns=['a','b','c'])

r1 = pd.rolling_mean(df['a'],2)
r2 = pd.rolling_mean(df['b'],2)
r = r1/r2
res = r.to_frame('test')


print r1,r2,res

print df
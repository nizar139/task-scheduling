import pandas as pd
import plotly.express as px
from pathlib import Path
import altair as alt
from altair_saver import save

names = ['smallRandom', 'xsmallComplex', 'smallComplex', 'mediumRandom',
         'MediumComplex', 'largeComplex', 'xlargeComplex', 'xxlargeComplex']
file_num = 0
path_name = 'schedules/{}.csv'.format(names[file_num])
filepath = Path(path_name)
filepath.parent.mkdir(parents=True, exist_ok=True)


def display_schedule(df_schedule):
    fig = px.timeline(df_schedule, x_start="Start", x_end="End", y="Core")
    # otherwise tasks are listed from the bottom up
    fig.update_yaxes(autorange="reversed")
    fig.show()


pd_schedule = pd.read_csv(filepath)
print(pd_schedule)
chart = alt.Chart(pd_schedule).mark_bar().encode(
    x='Start', x2='End', y='Core')
save_path = 'schedules/{}.png'.format(names[file_num])
save(chart, "chart.png")

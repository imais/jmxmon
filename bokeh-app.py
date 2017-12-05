import random
import socket
from datetime import datetime
# from bokeh.layouts import row, column, gridplot
from bokeh.models import ColumnDataSource, Slider, Select
from bokeh.plotting import curdoc, figure

DATA_SERVER_HOST = 'localhost'
DATA_SERVER_PORT = 9999
BUFFER_SIZE = 1024

data = ColumnDataSource(dict(time=[], val=[]))

plt = figure(plot_width=800,
			 plot_height=400,
			 x_axis_type='datetime',
			 tools="xpan,xwheel_zoom,xbox_zoom,reset",
			 title="Kafka BytesOutPerSec")
plt.line(source=data, x='time', y='val')
plt.xaxis.axis_label = "Time"
plt.yaxis.axis_label = "Outgoing Data Rate [Bytes/sec]"

def update_data():
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((DATA_SERVER_HOST, DATA_SERVER_PORT))
	sock.send('get_data\n')
	resp = sock.recv(BUFFER_SIZE)
	sock.close()
	new_val = resp.split(', ')[-1]
	time = [datetime.now()]
	val = [float(new_val)]
	data.stream(dict(time=time, val=val), 100)
	print("t={}, v={}".format(t, v))

curdoc().add_root(plt)
curdoc().add_periodic_callback(update_data, 1000)
curdoc().title = "Stream test"

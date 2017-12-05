import random
import socket
from datetime import datetime
# from bokeh.layouts import row, column, gridplot
from bokeh.models import ColumnDataSource, HoverTool, SaveTool
from bokeh.plotting import curdoc, figure

DATA_SERVER_HOST = 'localhost'
DATA_SERVER_PORT = 9999
BUFFER_SIZE = 1024

data = ColumnDataSource(dict(time=[], display_time=[], data_rate=[]))

hover = HoverTool(tooltips=[
    ("Time", "@display_time"),
    ("Data Rate", "@data_rate")])

plt = figure(plot_width=800,
			 plot_height=400,
			 x_axis_type='datetime',
			 tools="xpan, xwheel_zoom, xbox_zoom, save, reset",
			 title="Kafka BytesOutPerSec")
plt.line(source=data, x='time', y='data_rate')
plt.add_tools(hover)
plt.y_range.start = 0
plt.xaxis.axis_label = "Time"
plt.yaxis.axis_label = "Data Rate [Kbytes/sec]"


def update_data():
	# send request to local server
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((DATA_SERVER_HOST, DATA_SERVER_PORT))
	sock.send('get_data\n')
	resp = sock.recv(BUFFER_SIZE)
	sock.close()

	now = datetime.now()
	time = [now]
	display_time = [now.strftime("%m-%d-%Y %H:%M:%S.%f")]
	data_rate = [float(resp.split(', ')[-1]) / 1024] # bytes/s -> Kbytes/s

	data.stream(dict(time=time, display_time=display_time, data_rate=data_rate), 10000)
	print("time={}, data_rate={}".format(display_time, data_rate))

curdoc().add_root(plt)
curdoc().add_periodic_callback(update_data, 1000)
curdoc().title = "Kafka Metrics Visualizer"

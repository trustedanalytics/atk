#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import trustedanalytics as ta
ta.connect()

for name in ta.get_frame_names():
    print 'deleting frame: %s' %name
    ta.drop_frames(name)


employees_frame = ta.Frame(ta.CsvFile("employees.csv", schema = [('Employee', str), ('Manager', str), ('Title', str), ('Years', ta.int64)], skip_header_lines=1), 'employees_frame')

employees_frame.inspect()

#A bipartite graph
#Notice that this is a funny example since managers are also employees!
#Preseuambly Steve the manager and Steve the employee are the same person

#Option 1

graph = ta.Graph()
graph.define_vertex_type('Employee')
graph.define_edge_type('worksunder', 'Employee', 'Employee', directed=False)
graph.vertices['Employee'].add_vertices(employees_frame, 'Manager', [])
graph.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
graph.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'])

graph.vertex_count
graph.edge_count
graph.vertices['Employee'].inspect(9)
graph.edges['worksunder'].inspect(20)

#Option 2

ta.drop_graphs(graph)
graph = ta.Graph()
graph.define_vertex_type('Employee')
graph.define_edge_type('worksunder', 'Employee', 'Employee', directed=False)
graph.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
graph.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'], create_missing_vertices = True)

graph.vertex_count
graph.edge_count
graph.vertices['Employee'].inspect(9)
graph.edges['worksunder'].inspect(20)

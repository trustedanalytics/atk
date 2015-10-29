#
#  Copyright (c) 2015 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


import tornado.ioloop
import tornado.web
import argparse

parser = argparse.ArgumentParser(description="Get CDH configurations for ATK")
parser.add_argument("--host", type=str, help="Documentation Server Host address", default="0.0.0.0")
parser.add_argument("--port", type=int, help="Documentation Server Port ", default=80)
parser.add_argument("--path", type=str, help="Documentation path", default="admin")

args = parser.parse_args()

settings = {'debug': True,
            'static_path': args.path}

application = tornado.web.Application([
    (r"/", tornado.web.StaticFileHandler),
    ],**settings)

if __name__ == "__main__":
    application.listen(args.port, args.host)
    tornado.ioloop.IOLoop.instance().start()

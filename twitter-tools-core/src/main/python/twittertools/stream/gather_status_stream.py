# Twitter Tools
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import logging
import logging.handlers


consumer_key=""
consumer_secret=""

access_token=""
access_token_secret=""

class TweetListener(StreamListener):

  def __init__(self,api=None):
    super(TweetListener,self).__init__(api)
    self.logger = logging.getLogger('tweetlogger')

    
    statusHandler = logging.handlers.TimedRotatingFileHandler('status.log',when='H',encoding='bz2',utc=True)
    statusHandler.setLevel(logging.INFO)
    self.logger.addHandler(statusHandler)
    

    warningHandler = logging.handlers.TimedRotatingFileHandler('warning.log',when='H',encoding='bz2',utc=True)
    warningHandler.setLevel(logging.WARN)
    self.logger.addHandler(warningHandler)
    logging.captureWarnings(True);

    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.WARN)
    self.logger.addHandler(consoleHandler)


    self.logger.setLevel(logging.INFO)
    self.count = 0

  def on_data(self,data):
    self.count+=1
    self.logger.info(data)
    if self.count % 1000 == 0:
        print "%d statuses processed" % self.count
    return True

  def on_error(self,exception):
    self.logger.warn(str(exception))

if __name__ == '__main__':
  listener = TweetListener()
  auth = OAuthHandler(consumer_key,consumer_secret)
  auth.set_access_token(access_token,access_token_secret)

  stream = Stream(auth,listener)
  while True:
    try:
      stream.sample()
    except Exception as ex:
      print str(ex)
      pass

# Software License Agreement (BSD License)
#
# Copyright (c) 2012, Willow Garage, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above
#    copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided
#    with the distribution.
#  * Neither the name of Willow Garage, Inc. nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import os
import signal

from numpy.core.defchararray import index
import rospy
import rosbag
import time
import threading
import numpy as np
import pandas as pd
import os
from operator import attrgetter

from python_qt_binding.QtCore import Qt, QObject, QTimer, QProcess, qWarning, Signal
from python_qt_binding.QtWidgets import QGraphicsScene, QMessageBox

from multi_signals_annotator import bag_helper

from .bag_helper import get_topics, notNoneAttrs
from .player import Player


class BagTimeline(QObject):

    """
    BagTimeline contains bag files, all information required to display the bag data visualization
    on the screen Also handles events
    """
    status_bar_changed_signal = Signal()
    selected_region_changed = Signal(rospy.Time, rospy.Time)
    set_status_text = Signal(str)
    update_player_progress = Signal()

    def __init__(self, context, publish_clock):
        """
        :param context:
            plugin context hook to enable adding multi_signals_annotator plugin widgets as ROS_GUI snapin panes,
            ''PluginContext''
        """
        super(BagTimeline, self).__init__()
        self._bags = None
        self.bag_filename = None
        self._bag_lock = threading.RLock()

        self.background_task = None  # Display string
        self.background_task_cancel = False

        # Playing / Recording
        self._playhead_lock = threading.RLock()
        self._max_play_speed = 1024.0  # fastest X play speed
        self._min_play_speed = 1.0 / 1024.0  # slowest X play speed
        self._play_speed = 1.0
        self._playhead_positions = 0.0
        self._curr_timestamp = 0.0
        self._player = False
        self.last_frame = None
        self.last_playhead = None
        self.desired_playhead = None
        self.wrap = True  # should the playhead wrap when it reaches the end?
        self.stick_to_end = False  # should the playhead stick to the end?
        self._play_timer = QTimer()
        self._play_timer.timeout.connect(self.on_idle)
        self._play_timer.setInterval(3)

        self._context = context

        self._datatype_by_topic = None
        self._pyclass_by_topic = None

        self.background_progress = 0
        self.__closed = False

    def get_context(self):
        """
        :returns: the ROS_GUI context, 'PluginContext'
        """
        return self._context

    def handle_close(self):
        """
        Cleans up the timeline, bag and any threads
        """
        if self.__closed:
            return
        else:
            self.__closed = True
        # self._play_timer.stop()
        self.stop_publishing()

        if self._player:
            self._player.stop()
        if self.background_task is not None:
            self.background_task_cancel = True
        if self._bags is not None:
            self._bags.close()

    # Bag Management and access
    def add_bag(self, bag, filename):
        """
        creates an indexing thread for each new topic in the bag
        fixes the boarders and notifies the indexing thread to index the new items bags
        :param bag: ros bag file, ''rosbag.bag''
        """
        if self._bags is not None:
            self._bags.close()
            self._bags = None
            del self._bags
        self._bags = bag
        self.bag_filename = filename

        bag_topics = bag_helper.get_topics(bag)
        self._topics_by_datatype, self._datatype_by_topic = self._get_topics_and_datatypes()
        self._curr_timestamp = self._get_start_stamp().to_sec()
        self._set_playhead_position(0.0)
        self.play_speed = 1.0

    @notNoneAttrs('_bags')
    def duration(self):
        '''
        :return: float, timestamp in seconds, includes fractions of a second
        '''
        with self._bag_lock:
            return self._bags.get_end_time() - self._bags.get_start_time()

    @notNoneAttrs('_bags')
    def file_size(self):
        with self._bag_lock:
            return self._bags.size

    # TODO Rethink API and if these need to be visible
    @notNoneAttrs('_bags')
    def _get_start_stamp(self):
        """
        :return: first stamp in the bags, ''rospy.Time''
        """
        with self._bag_lock:
            start_stamp = None
            bag_start_stamp = bag_helper.get_start_stamp(self._bags)
            if bag_start_stamp is not None and \
                    (start_stamp is None or bag_start_stamp < start_stamp):
                start_stamp = bag_start_stamp
            return start_stamp

    @notNoneAttrs('_bags')
    def _get_end_stamp(self):
        """
        :return: last stamp in the bags, ''rospy.Time''
        """
        with self._bag_lock:
            end_stamp = None
            bag_end_stamp = bag_helper.get_end_stamp(self._bags)
            if bag_end_stamp is not None and (end_stamp is None or bag_end_stamp > end_stamp):
                end_stamp = bag_end_stamp
            return end_stamp

    @notNoneAttrs('_bags')
    def _get_topics(self):
        """
        :return: sorted list of topic names, ''list(str)''
        """
        with self._bag_lock:
            topics = set()
            for topic in bag_helper.get_topics(self._bags):
                topics.add(topic)
            return sorted(topics)

    @notNoneAttrs('_bags')
    def _get_topics_and_datatypes(self):
        """
        :return: dict of list of topics for each datatype, ''dict(datatype:list(topic))''
        """
        with self._bag_lock:
            topics_by_datatype = {}
            datatype_by_topic = dict()
            pyclass_by_topic = dict()
            extract_by_topic = dict()
            for datatype, topics in bag_helper.get_topics_by_datatype(self._bags).items():
                topics_by_datatype.setdefault(datatype, []).extend(topics)
                for topic in topics:
                    datatype_by_topic[topic] = datatype
            return topics_by_datatype, datatype_by_topic

    @notNoneAttrs('_bags')
    def get_datatype(self, topic):
        """
        :return: datatype associated with a topic, ''str''
        :raises: if there are multiple datatypes assigned to a single topic, ''Exception''
        """
        with self._bag_lock:
            datatype = None
            bag_datatype = bag_helper.get_datatype(self._bags, topic)
            if datatype and bag_datatype and (bag_datatype != datatype):
                raise Exception('topic %s has multiple datatypes: %s and %s' %
                                (topic, datatype, bag_datatype))
            if bag_datatype:
                datatype = bag_datatype
            return datatype

    @notNoneAttrs('_bags')
    def get_entries(self, topics, start_stamp, end_stamp):
        """
        generator function for bag entries
        :param topics: list of topics to query, ''list(str)''
        :param start_stamp: stamp to start at, ''rospy.Time''
        :param end_stamp: stamp to end at, ''rospy,Time''
        :returns: entries the bag file, ''msg''
        """
        with self._bag_lock:
            from rosbag import bag  # for _mergesort
            bag_entries = []

            bag_start_time = bag_helper.get_start_stamp(self._bags)
            if bag_start_time is not None and bag_start_time > end_stamp:
                raise IndexError
            bag_end_time = bag_helper.get_end_stamp(self._bags)
            if bag_end_time is not None and bag_end_time < start_stamp:
                raise IndexError
            connections = list(self._bags._get_connections(topics))
            bag_entries.append(self._bags._get_entries(connections, start_stamp, end_stamp))

            for entry, _ in bag._mergesort(bag_entries, key=lambda entry: entry.time):
                yield entry

    @notNoneAttrs('_bags')
    def get_entries_with_bags(self, topic, start_stamp, end_stamp):
        """
        generator function for bag entries
        :param topics: list of topics to query, ''list(str)''
        :param start_stamp: stamp to start at, ''rospy.Time''
        :param end_stamp: stamp to end at, ''rospy,Time''
        :returns: tuple of (bag, entry) for the entries in the bag file, ''(rosbag.bag, msg)''
        """
        with self._bag_lock:
            from rosbag import bag  # for _mergesort

            bag_entries = []
            bag_by_iter = {}
            bag_start_time = bag_helper.get_start_stamp(self._bags)
            if start_stamp is None:
                start_stamp = bag_start_time
            bag_end_time = bag_helper.get_end_stamp(self._bags)
            if end_stamp is None:
                end_stamp = bag_end_time
            if bag_start_time is not None and bag_start_time > end_stamp:
                raise IndexError
            if bag_end_time is not None and bag_end_time < start_stamp:
                raise IndexError
            connections = list(self._bags._get_connections(topic))
            it = iter(self._bags._get_entries(connections, start_stamp, end_stamp))
            bag_by_iter[it] = self._bags
            bag_entries.append(it)

            for entry, it in bag._mergesort(bag_entries, key=lambda entry: entry.time):
                yield bag_by_iter[it], entry

    @notNoneAttrs('_bags')
    def get_entry(self, t, topic):
        """
        Access a bag entry
        :param t: time, ''rospy.Time''
        :param topic: the topic to be accessed, ''str''
        :return: tuple of (bag, entry) corresponding to time t and topic, ''(rosbag.bag, msg)''
        """
        with self._bag_lock:
            entry_bag, entry = None, None

            bag_entry = self._bags._get_entry(t, self._bags._get_connections(topic))
            if bag_entry and (not entry or bag_entry.time > entry.time):
                entry_bag, entry = self._bags, bag_entry

            return entry_bag, entry

    @notNoneAttrs('_bags')
    def get_entry_before(self, t):
        """
        Access a bag entry
        :param t: time, ''rospy.Time''
        :return: tuple of (bag, entry) corresponding to time t, ''(rosbag.bag, msg)''
        """
        with self._bag_lock:
            entry_bag, entry = None, None
            bag_entry = self._bags._get_entry(t - rospy.Duration(0, 1), self._bags._get_connections())
            if bag_entry and (not entry or bag_entry.time < entry.time):
                entry_bag, entry = self._bags, bag_entry

            return entry_bag, entry

    @notNoneAttrs('_bags')
    def get_entry_after(self, t):
        """
        Access a bag entry
        :param t: time, ''rospy.Time''
        :return: tuple of (bag, entry) corisponding to time t, ''(rosbag.bag, msg)''
        """
        with self._bag_lock:
            entry_bag, entry = None, None
            bag_entry = self._bags._get_entry_after(t, self._bags._get_connections())
            if bag_entry and (not entry or bag_entry.time < entry.time):
                entry_bag, entry = self._bags, bag_entry

            return entry_bag, entry

    def start_background_task(self, background_task):
        """
        Verify that a background task is not currently running before starting a new one
        :param background_task: name of the background task, ''str''
        """
        if self.background_task is not None:
            QMessageBox(
                QMessageBox.Warning, 'Exclamation', 'Background operation already running:\n\n%s' %
                self.background_task, QMessageBox.Ok).exec_()
            return False

        self.background_task = background_task
        self.background_task_cancel = False
        return True

    def stop_background_task(self):
        self.background_task = None

    # Export selected message contents to CSV
    @notNoneAttrs('_bags')
    def extract_data_from_bag(self, topics_selection, path_to_save, start_stamp, end_stamp):
        self._export_selection_topics(topics_selection, path_to_save, start_stamp, end_stamp)

    def _export_selection_topics(self, topics_selection, path_to_save, start_stamp, end_stamp):
        """
        Starts a thread to save the current selection topics to files
        """
        if not self.start_background_task('Extract selected data from bag to "%s"' % path_to_save):
            return
        # TODO implement a status bar area with information on the current save status
        self.set_status_text.emit("Exporting......")
        bag_entries = list(self.get_entries_with_bags(topics_selection, start_stamp, end_stamp))

        if self.background_task_cancel:
            return

        # Get the total number of messages to copy
        total_messages = len(bag_entries)

        # If no messages, prompt the user and return
        if total_messages == 0:
            QMessageBox(QMessageBox.Warning, 'multi_signals_annotator', 'No messages found', QMessageBox.Ok).exec_()
            self.stop_background_task()
            return

        # generate filenames for writing
        folders_dict = dict()
        try:
            for topic in topics_selection:
                topic_path = os.path.join(path_to_save, topic[1:].replace('/', '.') + '.pkl')
                folders_dict[topic] = topic_path
        except Exception as e:
            QMessageBox(QMessageBox.Warning, 'multi_signals_annotator', 'Error create the folder {} for exporting: {}'.format(path_to_save, str(e)), QMessageBox.Ok).exec_()
            self.stop_background_task()
            return

        # Run copying in a background thread
        self._export_topics_thread = threading.Thread(
            target=self._run_export_selection_topics,
            args=(folders_dict, topics_selection, bag_entries))
        self._export_topics_thread.start()

    def _run_export_selection_topics(self, folders_dict, topics_selection, bag_entries):
        total_messages = len(bag_entries)
        update_step = max(1, total_messages / 100)
        message_num = 1
        progress = 0

        dataframe_by_topic = dict()
        ptr_by_topic = dict()
        for topic in topics_selection:
            datatype = self._datatype_by_topic[topic]
            msg_count = self._bags.get_message_count(topic)
            columns = ['timestamp', 'datatype']
            index = range(0, msg_count)
            ptr_by_topic[topic] = 0
            if datatype in bag_helper.msg_map:
                columns = columns + bag_helper.msg_map[datatype]['extract']['paras']
            else:
                columns = columns + ['msg']
            dataframe_by_topic[topic] = pd.DataFrame(columns=columns, index=index)

        for bag, entry in bag_entries:
            if self.background_task_cancel:
                break
            try:
                topic, msg, t = self.read_message(bag, entry.position)
                # print("{} / {}, topic: {}".format(str(message_num), str(total_messages), str(topic)))
                datatype = self._datatype_by_topic[topic]
                ptr = ptr_by_topic[topic]
                ptr_data = dataframe_by_topic[topic].iloc[ptr]
                ptr_data['timestamp'] = t.to_nsec()
                ptr_data['datatype'] = datatype
                if datatype in bag_helper.msg_map:
                    extras = dict()
                    if 'extras' in bag_helper.msg_map[datatype]['extract']:
                        extras = bag_helper.msg_map[datatype]['extract']['extras']
                    for para in bag_helper.msg_map[datatype]['extract']['paras']:
                        if para in extras:
                            ptr_data[para] = np.frombuffer(attrgetter(para)(msg), dtype=extras[para])
                        else:
                            ptr_data[para] = attrgetter(para)(msg)
                else:
                    ptr_data['msg'] = str(msg)
                ptr_by_topic[topic] = ptr_by_topic[topic] + 1
            except Exception as ex:
                qWarning('Error exporting message at position %s: %s' % (str(entry.position), str(ex)))
                self.stop_background_task()
                return

            if message_num % update_step == 0 or message_num == total_messages:
                new_progress = int(100.0 * (float(message_num) / total_messages))
                if new_progress != progress:
                    progress = new_progress
                    if not self.background_task_cancel:
                        self.background_progress = progress
                        self.status_bar_changed_signal.emit()

            message_num += 1

        try:
            self.background_progress = 0
            self.status_bar_changed_signal.emit()
            for topic in topics_selection:
                dataframe_by_topic[topic].to_pickle(folders_dict[topic])
                # print(dataframe_by_topic[topic].head())
        except Exception as e:
            QMessageBox(QMessageBox.Warning, 'multi_signals_annotator', 'Error saving dataframes [%s]: %s' % (
                "err", str(e)), QMessageBox.Ok).exec_()
        self.set_status_text.emit('Exporting selected topics has been done!')
        self.stop_background_task()

    def read_message(self, bag, position):
        with self._bag_lock:
            return bag._read_message(position)

    def start_publishing(self):
        if not self._player and not self._create_player():
            return False

        self._player.start_publishing(self._playhead_positions, self.play_speed)
        return True

    def stop_publishing(self):
        if not self._player:
            return False

        self._player.stop_publishing()
        return True

    def _set_playhead_position(self, pos):
        self._playhead_positions = pos
        self.update_player_progress.emit()

    def _create_player(self):
        if not self._player:
            try:
                self._player = Player(self)
                self._player.errorSignal.connect(self._handle_player_error)
                self._player.outputSignal.connect(self._handle_player_output)
            except Exception as ex:
                qWarning('Error starting player; aborting publish: %s' % str(ex))
                return False

        return True

    def _handle_player_output(self, ret):
        if 'Done.' in ret:
            self._player.is_publishing = False
        if self._player.is_publishing:
            result = ret.split(' ')
            for idx in range(0, len(result)):
                if result[idx] == u'Time:':
                    try:
                        self._curr_timestamp = float(result[idx + 1])
                        self.status_bar_changed_signal.emit()
                    except Exception:
                        pass
                if result[idx] == u'Duration:':
                    try:
                        self._set_playhead_position(self._player._playhead_offset + float(result[idx + 1]))
                    except Exception:
                        pass

    def _handle_player_error(self, ret):
        print(ret)

    # Playing
    def on_idle(self):
        pass

    # property: play_speed
    def _get_play_speed(self):
        return self._play_speed

    def _set_play_speed(self, play_speed):
        if play_speed == self._play_speed:
            return
        self._play_speed = min(self._max_play_speed, max(self._min_play_speed, play_speed))
        self.status_bar_changed_signal.emit()

    play_speed = property(_get_play_speed, _set_play_speed)

    def navigate_play(self):
        self.last_frame = rospy.Time.from_sec(time.time())
        # self._play_timer.start()
        self.start_publishing()

    def navigate_stop(self):
        # self._play_timer.stop()
        self.stop_publishing()

    def navigate_next(self):
        pass

    def navigate_fastforward(self):
        self.play_speed = self._play_speed * 2.0
        self.start_publishing()

    def navigate_rewind(self):
        self.play_speed = self._play_speed * 0.5
        self.start_publishing()

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

from __future__ import print_function
import os
import time

import rospy
import rospkg
import numpy as np
import pandas as pd


from python_qt_binding import loadUi
from python_qt_binding.QtCore import qDebug, QFileInfo, Qt, qWarning, Signal
from python_qt_binding.QtGui import QIcon, QResizeEvent, QStandardItem, QStandardItemModel, QDoubleValidator
from python_qt_binding.QtWidgets import QFileDialog, QGraphicsView, QWidget, QMessageBox, QLineEdit, QPushButton, QSlider, QHeaderView

import rosbag
from .bag_helper import stamp_to_str, filesize_to_str, SLIDER_BAR_MAX
from .bag_timeline import BagTimeline


class BagGraphicsView(QGraphicsView):

    def __init__(self, parent=None):
        super(BagGraphicsView, self).__init__()


class BagWidget(QWidget):

    """
    Widget for use with Bag class to display and replay bag files
    Handles all widget callbacks and contains the instance of BagTimeline for storing visualizing bag data
    """

    last_open_dir = os.getcwd()
    last_open_saving_dir = os.getcwd()
    set_status_text = Signal(str)

    def __init__(self, context, publish_clock):
        """
        :param context: plugin context hook to enable adding widgets as a ROS_GUI pane, ''PluginContext''
        """
        super(BagWidget, self).__init__()
        rp = rospkg.RosPack()
        ui_file = os.path.join(rp.get_path('multi_signals_annotator'), 'resource', 'bag_widget.ui')
        loadUi(ui_file, self, {'BagGraphicsView': BagGraphicsView})

        self.setObjectName('BagWidget')

        self._timeline = BagTimeline(context, publish_clock)

        self.topicsListViewModel = QStandardItemModel()

        self.play_icon = QIcon.fromTheme('media-playback-start')
        self.pause_icon = QIcon.fromTheme('media-playback-pause')
        self.play_button.setIcon(self.play_icon)
        self.slower_button.setIcon(QIcon.fromTheme('media-seek-backward'))
        self.faster_button.setIcon(QIcon.fromTheme('media-seek-forward'))

        self.play_button.clicked[bool].connect(self._handle_play_clicked)
        self.faster_button.clicked[bool].connect(self._handle_faster_clicked)
        self.slower_button.clicked[bool].connect(self._handle_slower_clicked)
        self.pushButton_load.clicked[bool].connect(self._handle_load_clicked)
        self.pushButton_choose_export_path.clicked[bool].connect(self._handel_chosse_export_path)
        self.pushButton_export.clicked[bool].connect(self._handle_save_clicked)
        self.pushButton_selectAll.clicked[bool].connect(self._handle_select_all)
        self.pushButton_unSelectAll.clicked[bool].connect(self._handle_unselect_all)

        self.slider_status = 'released'
        self.horizontalSlider.sliderPressed.connect(self._handle_slider_pressed)
        self.horizontalSlider.sliderReleased.connect(self._handle_slider_released)
        self.horizontalSlider.valueChanged.connect(self._handle_slider_valueChanged)
        self.horizontalSlider.setMinimum(0)
        self.horizontalSlider.setMaximum(SLIDER_BAR_MAX)
        self.horizontalSlider.setSingleStep(1)
        self.horizontalSlider.setTickPosition(QSlider.TicksBelow)
        self.horizontalSlider.setTickInterval(SLIDER_BAR_MAX / 100)

        self.lineEdit_playhead_status = 'finish'
        self.lineEdit_playhead.textEdited.connect(self._handle_lineEdit_playhead_textEdited)
        self.lineEdit_playhead.selectionChanged.connect(self._handle_lineEdit_playhead_selectionChanged)
        self.lineEdit_playhead.cursorPositionChanged.connect(self._handle_lineEdit_playhead_cursorPositionChanged)
        self.lineEdit_playhead.returnPressed.connect(self._handle_lineEdit_playhead_returnPressed)
        self.lineEdit_playhead.editingFinished.connect(self._handle_lineEdit_playhead_editingFinished)
        self.lineEdit_playhead.setValidator(QDoubleValidator(0.0, 100.0, 4))

        self.closeEvent = self.handle_close

        self.topicsListView.clicked.connect(self._handle_topicsList_clicked)

        self.tableWidget.setColumnCount(3)
        self.tableWidgetmodel = QStandardItemModel()
        self.tableWidget.setHorizontalHeaderLabels(['Timestamp', 'Tag', 'Operation'])
        tableWidgetHeader = self.tableWidget.horizontalHeader()
        tableWidgetHeader.setSectionResizeMode(0, QHeaderView.Stretch)
        tableWidgetHeader.setSectionResizeMode(1, QHeaderView.Stretch)
        tableWidgetHeader.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.tableData = pd.DataFrame(columns=['Timestamp', 'Tag'])

        self.pushButton_Add_Tag.clicked[bool].connect(self._handle_addTag)
        self.pushButton_Clear_All_Tags.clicked[bool].connect(self._handle_clearAllTags)
        self.pushButton_Save_All_Tags.clicked[bool].connect(self._handle_saveAllTags)

        # TODO when the closeEvent is properly called by ROS_GUI implement that
        # event instead of destroyed
        self.destroyed.connect(self.handle_destroy)

        self.play_button.setEnabled(False)
        self.faster_button.setEnabled(False)
        self.slower_button.setEnabled(False)
        self.pushButton_export.setEnabled(False)
        self.horizontalSlider.setEnabled(False)
        self.lineEdit_playhead.setEnabled(False)
        self.pushButton_selectAll.setEnabled(False)
        self.pushButton_unSelectAll.setEnabled(False)
        self.pushButton_Add_Tag.setEnabled(False)
        self.pushButton_Save_All_Tags.setEnabled(False)
        self.tableWidget.setEnabled(False)
        self.pushButton_choose_export_path.setEnabled(False)

        self.lineEdit_bag_file_path.setText(self.last_open_dir)
        self.lineEdit_export_path.setText(self.last_open_saving_dir)

        self._timeline.status_bar_changed_signal.connect(self._update_status_bar)
        self._timeline.set_status_text.connect(self._set_status_text)
        self.set_status_text.connect(self._set_status_text)
        self._timeline.update_player_progress.connect(self._update_player_progress)

        self.bagfile_name = None

    def handle_destroy(self, args):
        pass

    def handle_close(self, event):
        self.shutdown_all()
        event.accept()

    def _handle_addTag(self):
        self._append_one_row()
        self.tableData = self.tableData.append({'Timestamp': str(self._timeline._curr_timestamp), 'Tag': 'None'}, ignore_index=True)

    def _append_one_row(self):
        self.tableWidget.setRowCount(self.tableWidget.rowCount() + 1)

        tableItem = QLineEdit()
        tableItem.setValidator(QDoubleValidator(0.0, np.inf, 9))
        tableItem.setText('{:.9f}'.format(self._timeline._curr_timestamp))
        tableItem.setAlignment(Qt.AlignCenter)
        tableItem.editingFinished.connect(self._clicked_row_Timestamp_editingFinished)
        self.tableWidget.setCellWidget(self.tableWidget.rowCount() - 1, 0, tableItem)

        tableItem = QLineEdit()
        tableItem.setText('None')
        tableItem.setAlignment(Qt.AlignCenter)
        tableItem.editingFinished.connect(self._clicked_row_Tag_editingFinished)
        self.tableWidget.setCellWidget(self.tableWidget.rowCount() - 1, 1, tableItem)

        pressButton_del = QPushButton(self.tableWidget)
        pressButton_del.setText('Delete')
        pressButton_del.setMinimumWidth(200)
        pressButton_del.clicked[bool].connect(self._delete_clicked_row)
        self.tableWidget.setCellWidget(self.tableWidget.rowCount() - 1, 2, pressButton_del)

    def _delete_clicked_row(self):
        button = self.sender()
        if button:
            row = self.tableWidget.indexAt(button.pos()).row()
            self.tableWidget.removeRow(row)
            # self.tableData.drop(index=self.tableData.index[[row]], inplace=True)
            if row in self.tableData.index:
                self.tableData.drop(row, inplace=True)
                self.tableData.reset_index(drop=True, inplace=True)

    def _clicked_row_Timestamp_editingFinished(self):
        sender = self.sender()
        if sender:
            row = self.tableWidget.indexAt(sender.pos()).row()
            if row in self.tableData.index:
                self.tableData.loc[row]['Timestamp'] = sender.text()

    def _clicked_row_Tag_editingFinished(self):
        sender = self.sender()
        if sender:
            row = self.tableWidget.indexAt(sender.pos()).row()
            if row in self.tableData.index:
                self.tableData.loc[row]['Tag'] = sender.text()

    def _handle_clearAllTags(self):
        returnValue = QMessageBox(QMessageBox.Information, 'multi_signals_annotator', 'Are you sure to clear all tags?', QMessageBox.Ok | QMessageBox.Cancel).exec_()
        if returnValue == QMessageBox.Ok:
            while(self.tableWidget.rowCount() > 0):
                self.tableWidget.removeRow(0)
            self.tableData = None
            del self.tableData
            self.tableData = pd.DataFrame(columns=['Timestamp', 'Tag'])

    def _handle_saveAllTags(self):
        if self.last_open_saving_dir and os.path.exists(self.last_open_saving_dir):
            path_to_save = os.path.join(self.last_open_saving_dir, self.bagfile_name)
            try:
                if not os.path.exists(path_to_save):
                    os.mkdir(path_to_save)
                self.tableData.to_csv(os.path.join(path_to_save, 'Tags.csv'))
                QMessageBox(QMessageBox.Information, 'multi_signals_annotator', 'All tags have been saved!', QMessageBox.Ok).exec_()
            except Exception as e:
                QMessageBox(QMessageBox.Warning, 'multi_signals_annotator', 'Error create the folder {} for exporting: {}'.format(path_to_save, str(e)), QMessageBox.Ok).exec_()

    def _handle_slider_pressed(self):
        self.slider_status = 'pressed'

    def _handle_slider_released(self):
        if self.slider_status == 'valueChanged':
            self.change_playhead_position(1.0 / SLIDER_BAR_MAX * self.horizontalSlider.value() * self._timeline.duration())
        self.slider_status = 'released'

    def _handle_slider_valueChanged(self):
        if self.slider_status == 'released':
            self.change_playhead_position(1.0 / SLIDER_BAR_MAX * self.horizontalSlider.value() * self._timeline.duration())
        elif self.slider_status == 'pressed':
            self.slider_status = 'valueChanged'

    def _handle_lineEdit_playhead_textEdited(self):
        self.lineEdit_playhead_status = 'edit'

    def _handle_lineEdit_playhead_returnPressed(self):
        self.lineEdit_playhead_status = 'return'
        try:
            self.change_playhead_position(float(self.lineEdit_playhead.text()))
        except Exception as e:
            print(e)

    def _handle_lineEdit_playhead_editingFinished(self):
        self.lineEdit_playhead_status = 'finish'

    def _handle_lineEdit_playhead_selectionChanged(self):
        self.lineEdit_playhead_status = 'select'

    def _handle_lineEdit_playhead_cursorPositionChanged(self, old, new):
        self.lineEdit_playhead_status = 'position_changed'

    def change_playhead_position(self, new_position):
        try:
            republish = False
            if self._timeline._player and self._timeline._player.is_publishing:
                self._timeline.stop_publishing()
                republish = True

            self._timeline._playhead_positions = new_position
            self._update_player_progress()
            if republish:
                self._timeline.start_publishing()
        except Exception as e:
            print(e)

    def _handle_play_clicked(self, checked):
        if checked:
            self.play_button.setIcon(self.pause_icon)
            self._timeline.navigate_play()
        else:
            self.play_button.setIcon(self.play_icon)
            self._timeline.navigate_stop()

    def _handle_faster_clicked(self):
        self._timeline.navigate_fastforward()
        self.play_button.setChecked(True)
        self.play_button.setIcon(self.pause_icon)

    def _handle_slower_clicked(self):
        self._timeline.navigate_rewind()
        self.play_button.setChecked(True)
        self.play_button.setIcon(self.pause_icon)

    def _handle_topicsList_clicked(self, qModelIndex):
        row = qModelIndex.row()
        if self.topicsListViewModel.item(row).checkState() == Qt.Unchecked:
            self.topicsListViewModel.item(row).setCheckState(Qt.Checked)
        elif self.topicsListViewModel.item(row).checkState() == Qt.Checked:
            self.topicsListViewModel.item(row).setCheckState(Qt.Unchecked)

    def _handle_select_all(self):
        for i in range(self.topicsListViewModel.rowCount()):
            item = self.topicsListViewModel.item(i)
            item.setCheckState(Qt.Checked)

    def _handle_unselect_all(self):
        for i in range(self.topicsListViewModel.rowCount()):
            item = self.topicsListViewModel.item(i)
            item.setCheckState(Qt.Unchecked)

    def _handle_load_clicked(self):
        if os.path.exists(self.lineEdit_bag_file_path.text()):
            self.last_open_dir = self.lineEdit_bag_file_path.text()
        filename,  _ = QFileDialog.getOpenFileName(self, self.tr('Load from Files'), self.last_open_dir, self.tr('Bag files {.bag} (*.bag)'))
        last_topics_selection = self._get_selected_topics()
        if filename and os.path.exists(filename):
            self.topicsListViewModel.clear()
            self.last_open_dir = QFileInfo(filename).absoluteDir().absolutePath()
            self.lineEdit_bag_file_path.setText(self.last_open_dir)
            self.bagfile_name = os.path.basename(filename)[0:-4]
            self.label_current_rosbag_file.setText(self.bagfile_name)
            self.load_bag(filename)

            self.label_max_timeline.setText('{:.4f}'.format(self._timeline.duration()))

            for topic in self._timeline._get_topics():
                item = QStandardItem(topic)
                item.setEditable(False)
                item.setCheckable(False)
                check = Qt.Checked if topic in last_topics_selection else Qt.Unchecked
                item.setCheckState(check)
                self.topicsListViewModel.appendRow(item)
            self.topicsListView.setModel(self.topicsListViewModel)
            self._update_status_bar()

            while(self.tableWidget.rowCount() > 0):
                self.tableWidget.removeRow(0)
            self.tableData = None
            del self.tableData
            self.tableData = pd.DataFrame(columns=['Timestamp', 'Tag'])

    def load_buttons_status(self, status):
        self.pushButton_load.setEnabled(status)
        self.play_button.setEnabled(status)
        self.faster_button.setEnabled(status)
        self.slower_button.setEnabled(status)
        self.pushButton_export.setEnabled(status)
        self.horizontalSlider.setEnabled(status)
        self.lineEdit_playhead.setEnabled(status)
        self.pushButton_selectAll.setEnabled(status)
        self.pushButton_unSelectAll.setEnabled(status)
        self.pushButton_Add_Tag.setEnabled(status)
        self.pushButton_Save_All_Tags.setEnabled(status)
        self.tableWidget.setEnabled(status)
        self.pushButton_choose_export_path.setEnabled(status)

    def load_bag(self, filename):
        qDebug("Loading '%s'..." % filename.encode(errors='replace'))
        self.set_status_text.emit("Loading '%s'..." % filename)

        try:
            bag = rosbag.Bag(filename)
            self.load_buttons_status(False)
            self._timeline.add_bag(bag, filename)
            self.load_buttons_status(True)
            self.lineEdit_playhead.setValidator(QDoubleValidator(0.0, self._timeline.duration(), 4))
            # put the progress bar back the way it was
            qDebug("Done loading '%s'" % filename.encode(errors='replace'))
            self.set_status_text.emit("Bag file has been loaded.")
        except rosbag.ROSBagException as e:
            print("Loading '%s' failed due to: %s" % (filename.encode(errors='replace'), e))
            self.set_status_text.emit("Loading '%s' failed due to: %s" % (filename, e))

    def _get_selected_topics(self):
        topics = self._timeline._get_topics()
        topics_selection = [
            topics[i]
            for i in range(self.topicsListViewModel.rowCount())
            if self.topicsListViewModel.item(i).checkState() == Qt.Checked
        ]
        return topics_selection

    def _handel_chosse_export_path(self):
        if os.path.exists(self.lineEdit_export_path.text()):
            self.last_open_saving_dir = self.lineEdit_export_path.text()
        path_to_save = QFileDialog.getExistingDirectory(self, self.tr('Choose dir to save data'), self.last_open_saving_dir)
        if path_to_save and os.path.exists(path_to_save):
            self.last_open_saving_dir = path_to_save
            self.lineEdit_export_path.setText(self.last_open_saving_dir)

    def _handle_save_clicked(self):
        topics_selection = self._get_selected_topics()
        if topics_selection is not None and topics_selection != [] and None not in topics_selection:
            start_stamp = None
            end_stamp = None
            if self.last_open_saving_dir and os.path.exists(self.last_open_saving_dir):
                path_to_save = os.path.join(self.last_open_saving_dir, self.bagfile_name)
                try:
                    if not os.path.exists(path_to_save):
                        os.mkdir(path_to_save)
                    self._timeline.extract_data_from_bag(topics_selection, path_to_save, start_stamp, end_stamp)
                except Exception as e:
                    QMessageBox(QMessageBox.Warning, 'multi_signals_annotator', 'Error create the folder {} for exporting: {}'.format(path_to_save, str(e)), QMessageBox.Ok).exec_()
        else:
            QMessageBox(QMessageBox.Warning, 'multi_signals_annotator', 'Please select 1 topic at least.', QMessageBox.Ok).exec_()

    def _set_status_text(self, text):
        self.progress_bar.setTextVisible(False)
        if text:
            self.progress_bar.setFormat(text)
            self.progress_bar.setTextVisible(True)

    def _update_player_progress(self):
        if self.slider_status == 'released':
            self.horizontalSlider.blockSignals(True)
            self.horizontalSlider.setValue(1.0 * SLIDER_BAR_MAX * self._timeline._playhead_positions / self._timeline.duration())
            self.horizontalSlider.blockSignals(False)
        if self.lineEdit_playhead_status == 'finish':
            self.lineEdit_playhead.blockSignals(True)
            self.lineEdit_playhead.setText('{:.4f}'.format(self._timeline._playhead_positions))
            self.lineEdit_playhead.blockSignals(False)

    def _update_status_bar(self):
        try:
            # Background Process Status
            self.progress_bar.setValue(self._timeline.background_progress)

            # Raw timestamp
            self.stamp_label.setText('%.9fs' % self._timeline._curr_timestamp)

            # Human-readable time
            self.date_label.setText(stamp_to_str(self._timeline._curr_timestamp))

            # Elapsed time (in seconds)
            self.seconds_label.setText('%.3fs' % (self._timeline.duration() - self._timeline._playhead_positions))

            # File size
            self.filesize_label.setText(filesize_to_str(self._timeline.file_size()))

            # Play speed
            spd = self._timeline.play_speed
            self.playspeed_label.setText('x {:.4f}'.format(spd))
        except Exception as e:
            print(e)
    # Shutdown all members

    def shutdown_all(self):
        self._timeline.handle_close()

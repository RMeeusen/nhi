#  -*- coding: utf-8 -*-
#   Copyright notice
#     --------------------------------------------------------------------
#     Copyright (C) 2019 Deltares
#         Rob Rikken
#         Rob.Rikken@deltares.nl
#   #
#     This library is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#   #
#     This library is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#   #
#     You should have received a copy of the GNU General Public License
#     along with this library.  If not, see <http://www.gnu.org/licenses/>.
#     --------------------------------------------------------------------
#   #
#   This tool is part of <a href="http://www.OpenEarth.eu">OpenEarthTools</a>.
#   OpenEarthTools is an online collaboration to share and manage data and
#   programming tools in an open source, version controlled environment.
#   Sign up to receive regular updates of this function, and to contribute
#   your own tools.
#   #
#
import datetime
from threading import Thread

from detectors import *

print("Starting error checker")
print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Instantiate all the detectors.
detectors = [
    AfvoergebiedDetector.AfvoergebiedDetector(),
    BodemvalDetector.BodemvalDetector(),
    BrugDetector.BrugDetector(),
    CrossSectionLinesDetector.CrossSectionLinesDetector(),
    DuikersifonhevelDetector.DuikersifonhevelDetector(),
    DwarsprofielDetector.DwarsprofielDetector(),
    GemaalDetector.GemaalDetector(),
    HydroobjectDetector.HydroobjectDetector(),
    LateraleknoopDetector.LateraleknoopDetector(),
    MeetlocatieDetector.MeetlocatieDetector(),
    PompDetector.PompDetector(),
    StuwDetector.StuwDetector()
]

# List for the threads
threads = []

# The ahn threads are managed by the AHN helper. Instantiate this helper and add the thread to the threads list.
number_of_ahn_threads = 30
ahn_detection_helper = AhnDetectorHelper.AhnDetectorHelper()
ahn_thread = Thread(target=ahn_detection_helper.check_tiles_using_threads, args=(number_of_ahn_threads,))
threads.extend([ahn_thread])

for detector in detectors:
    # Remove the errors that are already in the table, so no double or stale errors will occur.
    detector.remove_old_errors()
    # Save the threads in the list so we can call a start and a join on them.
    threads.extend(detector.build_threads())

# Start the detector threads
for thread in threads:
    thread.start()

# Wait for all the threads to finish.

for thread in threads:
    thread.join()

print("Exiting Main Thread")
print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

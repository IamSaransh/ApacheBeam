# LogAnalyzer
Analyses error stats reporter from application and sends out reports of how many error per class

Polls for the logs incoming into a directory, makes a resport of the classNames corresponding to different error levels and how many times that class appeared for that log level.

Dumps that in a HTML table format in a local file.

This file is then read and attached to a HTML report stored in resources folder and sent to the Admin so that he can monitor the status of his/her applicaton.

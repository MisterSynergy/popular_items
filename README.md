# Popular items on Wikidata's main page
Wikidata bot that updates a list of popular items that is displayed on the main page.

This is a replacement script for the [popularItems.py](https://github.com/Pascalco/DeltaBot/blob/master/popularItems.py) script by User:Pasleim. His bot sometimes fails (as eg. discussed in Aug 2022 on WD:PC), so that MsynBot could replace it at any time if necessary. This is a completely new implementation, yet it has taken inspiration from Pasleim's version in order to be compatible in its resulting output.

The Wikidata page edited by this bot is [Wikidata:Main Page/Popular](https://www.wikidata.org/wiki/Wikidata:Main_Page/Popular).

## Technical requirements
This bot is currently not scheduled to run regularly. It is, however, available on [Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge) in the `msynbot` tool account in order to be kicked-off if necessary. It depends on the [shared pywikibot files](https://wikitech.wikimedia.org/wiki/Help:Toolforge/Pywikibot#Using_the_shared_Pywikibot_files_(recommended_setup)) and is running in a Kubernetes environment using Python 3.11.2.

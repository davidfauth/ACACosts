

# ACA Health care costs project

Data.Healthcare.Gov released QHP cost information for various health care plans for states in the Federally-Facilitated and State-Partnership Marketplaces. The data is available in a variety of formats and lays out costs for various levels of health care plans (Gold, Silver, Bronze and Catastrophe) for different categories.

This project looks at the costs, calculates averages, maximum and minimum costs, variance and standard deviations across the data.

# Mortar Ddata
Mortar is a platform-as-a-service for Hadoop.  With Mortar, you can run jobs on Hadoop using Apache Pig and Python without any special training.  You create your project using the Mortar Development Framework, deploy code using the Git revision control system, and Mortar does the rest.

## Getting Started

To start testing out this data:

1. [Signup for a Mortar account](https://app.mortardata.com/signup)
1. [Install the Mortar Development Framework](http://help.mortardata.com/#!/install_mortar_development_framework)
1.  Clone this repository to your computer and register it as a project with Mortar:

        git clone git@github.com:davidfauth/ACACosts
        cd ACACosts
        mortar register mortar-examples

Once you've setup the project, use the `mortar illustrate` command to show data flowing through a given script.  Use `mortar run` to run the script on a Hadoop cluster.

For lots more help and tutorials on running Mortar, check out the [Mortar Help](http://help.mortardata.com/) site.



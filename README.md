## Simple Azure Databricks Cost Analysis 


**TL;DR: This repository shows an example of leveraging Databricks tags to distribute instance pool cost among jobs run on it.**

For Azure Databricks Cost Analysis, the recommendation is to group costs by using resource tags in Azure, which can be even enforced by compute policies. This is especially helpful if resources are spread over multiple hierarchies in Azure or are shared by teams. It is possible to filter costs associated with a specific team, project, ect., by making sure that all resources used by one entity have a common tag. 

There are three types of tags for Azure Databricks: Workspace, cluster and pool tags. Tags are inherited by child resources that are used by a tagged resource. This means all clusters in a workspace will inherit the workspace tag and VMs and other infrastructure of a cluster will inherit the cluster and workspace tag.

One exception are instance pools. Tags have to be defined before creating a resource and since pools are constantly running, tags do not propagate to the cluster infrastructure. This is shown in the image below, more info [here](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/account-settings/usage-detail-tags#tag-propagation).

![tag propagation](https://github.com/wsilveira-usp/azure_dbx_cost_analysis/blob/main/image/tag-propagation.png?raw=true)

The Cost Analysis function of the Azure Cost Management tool allows you to analyze all costs associated with your Azure Databricks usage, such as:
- Total price for DBUs consumption
- DBU costs per workspace
- DBU costs per cluster type
- Infrastructure costs per workspace
- Costs per team or project using tags
- Total costs - DBUs plus infrastructure
- Storage costs: 
  - It is not possible to separate storage costs caused by Azure Databricks when using shared storage accounts.

For charging back VM cost under Instance Pools, upon Instance Pools creation give them ID tags. Apply these same ID tags to jobs run per Instance Pools. In this way, it is possible to know which job has run on each Instance Pool. Furthermore, finding the ratio of a job DBU usage against all jobs DBU usage in the Instance Pool will provide the Instance Pool usage percentage of that specific job. Recall that DBU usage is proportional to cluster size and execution time. Finally, multiply the Instance Pool usage percentage of a specific job by the Instance Pool total cost to chargeback the job. In terms of formula:

$$ \left( jobDBUCost_a \over allJobsDBUCostInstancePool_a \right) * InstancePoolTotalCost_a $$

#### Cost Export

The cost management tool shows the costs associated with DBU consumption and infrastructure. If you instead want to know the number of DBUs consumed, for example to estimate which pre-purchase option fits best for your usage, or holistically split an instance pool VM cost by jobs run on it, a cost export can give a more detailed overview. Using cost exports also allows you to build customized dashboards, automate chargeback or set cost alarms with scheduled exports. 

To create a cost export from the Azure Cost Management tool: 
- Go to “Configuration” → “Exports”.
- Select the required scope for your report and click “Add”.
- Select a name, the correct metric depending on your payment plan (actual or amortized costs), the export type (one-time or recurring export) and the time period of interest.
- The report will be saved to a storage account as csv file. Enter the details for the desired location.
- This csv file can then be consumed by Databricks to create chargeback dashboard based on the desired aggregations
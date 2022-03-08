# Access Control

One key challenge when managing features within the enterprise is to strike a balance between sharing and reusing features and controlling who has access to which features. Particular attention needs to be placed on access to features containing or built on PII (Personally Identifiable Information).

This document describes how Hopsworks project based multi-tenancy allows organization to achieve a balance between sharing features and protecting sensitive information.

# Project based multi-tenancy

Work in a Hopsworks deployment is organized within _projects_. A project is a collection of feature groups, training datasets, storage connectors, users and feature engineering code. Only members of the project have access to the content of the project. A more detailed documentation on project, members and roles is available in the [Project documentation](../hopsworks/latest/compute/project/multiTenancy/)

# Sharing

While data is available only within the context of a project, a project feature store can be shared with other projects. This has the effect of giving access to features, training datasets and storage connectors to the users of the other project.

Only data owner (see project roles in [Project documentation](../hopsworks/latest/compute/project/multiTenancy/)) can share the project feature store with other projects. Sharing a feature store can be done from _General_ section of the project settings by clicking on the _share feature store_ button.

<p align="center">
  <figure>
    <img src="../../../assets/images/feature_registry/share.png" alt="Share feature store">
    <figcaption>Share feature store</figcaption>
  </figure>
</p>

Select the project you want to share the feature store with, and the access rights to give to the users of the other project.


# Discovery

How users can discover features they don't have access to

# Patterns

What kind of patterns can be adopted to map the organization

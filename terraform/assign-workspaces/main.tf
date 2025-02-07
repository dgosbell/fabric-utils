variable capacity_map {
	type = list(object({
		capacity_name = string
		workspaces = list(string)
	}))
	default = [
		{
			capacity_name = "myf2" 
			workspaces = ["WS1","WS2"]
		},
		{
			capacity_name = "myf2se"
			workspaces = ["WS3","WS4"]
		}
	]
}

locals {
	foo = "bar"
	workspaces = flatten([
		for cap in var.capacity_map: [
			for ws in cap.workspaces: {
				capacity_name = cap.capacity_name
				workspace = ws
			}
		]
	])
}

data "fabric_capacity" "premium_capacity" {
  for_each = toset([for cap in var.capacity_map: cap.capacity_name])

  display_name = each.value
  
  lifecycle {
    postcondition {
      condition     = self.state == "Active"
      error_message = "Fabric Capacity ${each.value} is not in Active state. Please check the Fabric Capacity status."
    }
  }
}


# Create / Update a Fabric Workspace.
# https://registry.terraform.io/providers/microsoft/fabric/latest/docs/resources/workspace
resource "fabric_workspace" "workspace_assignment" {
  for_each = {for ws in local.workspaces: ws.workspace => ws}
  capacity_id  = data.fabric_capacity.premium_capacity[each.value.capacity_name].id
  display_name = each.key
}
resource "random_password" "warehouse" {
  length           = 16
  special          = false
}

variable "warehouse_username" {
    
    type        = string
    default     = "joe"
}
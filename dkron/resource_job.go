package dkron

import (
	"context"

	dkron "github.com/distribworks/dkron/v4/client"
	"github.com/distribworks/dkron/v4/types"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func resourceDkronJob() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceDkronJobCreate,
		ReadContext:   resourceDkronJobRead,
		UpdateContext: resourceDkronJobUpdate,
		DeleteContext: resourceDkronJobDelete,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"timezone": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"displayname": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"schedule": {
				Type:     schema.TypeString,
				Required: true,
			},
			"owner": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"owner_email": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"disabled": {
				Type:     schema.TypeBool,
				Optional: true,
			},
			"tags": {
				Type:     schema.TypeMap,
				Elem:     schema.TypeString,
				Optional: true,
			},
			"retries": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"parent_job": {
				Type:     schema.TypeString,
				Optional: true,
			},

			"concurrency": {
				Type:     schema.TypeString,
				Optional: true,
			},

			"executor": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringInSlice([]string{"gcppubsub", "grpc", "http", "kafka", "nats", "rabbitmq", "shell"}, false),
			},
			"executor_config": {
				Type:     schema.TypeMap,
				Elem:     schema.TypeString,
				Optional: true,
			},

			"metadata": {
				Type:     schema.TypeMap,
				Elem:     schema.TypeString,
				Optional: true,
			},

			"processors": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"type": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice([]string{"files", "log", "syslog"}, false),
						},
						"forward": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"log_dir": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},
		},
	}
}

func resourceDkronJobCreate(ctx context.Context, d *schema.ResourceData, m any) diag.Diagnostics {
	var diags diag.Diagnostics

	config := m.(DkronConfig)
	client, err := dkron.NewClient(config.host)
	if err != nil {
		return diag.FromErr(err)
	}

	body := dkron.CreateOrUpdateJobJSONRequestBody{
		Ephemeral: false,
	}

	body.Name = d.Get("name").(string)
	body.Timezone = d.Get("timezone").(string)
	body.Displayname = d.Get("displayname").(string)
	body.Schedule = d.Get("schedule").(string)
	body.Owner = d.Get("owner").(string)
	body.OwnerEmail = d.Get("owner_email").(string)
	body.Disabled = d.Get("disabled").(bool)
	body.Tags = d.Get("tags").(map[string]string)
	body.Retries = d.Get("retries").(uint32)
	body.ParentJob = d.Get("parent_job").(string)
	body.Concurrency = d.Get("concurrency").(string)
	body.Executor = d.Get("executor").(string)
	body.ExecutorConfig = d.Get("executor_config").(map[string]string)
	body.Metadata = d.Get("metadata").(map[string]string)

	processors := d.Get("processors").([]any)
	for _, p := range processors {
		processor := p.(map[string]string)

		ty := processor["type"]
		delete(processor, "type")

		if processor["forward"] == "" {
			delete(processor, "forward")
		}
		if processor["log_dir"] == "" {
			delete(processor, "log_dir")
		}

		body.Processors[ty] = &types.PluginConfig{
			Config: processor,
		}
	}

	if _, err := client.CreateOrUpdateJob(ctx, nil, body); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func resourceDkronJobRead(ctx context.Context, d *schema.ResourceData, m any) diag.Diagnostics {
	var diags diag.Diagnostics

	config := m.(DkronConfig)
	client, err := dkron.NewClientWithResponses(config.host)
	if err != nil {
		return diag.FromErr(err)
	}

	response, err := client.ShowJobByNameWithResponse(ctx, d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	job := response.JSON200

	d.Set("name", job.Name)
	d.Set("timezone", job.Timezone)
	d.Set("displayname", job.Displayname)
	d.Set("schedule", job.Schedule)
	d.Set("owner", job.Owner)
	d.Set("owner_email", job.OwnerEmail)
	d.Set("disabled", job.Disabled)
	d.Set("tags", job.Tags)
	d.Set("retries", job.Retries)
	d.Set("parent_job", job.ParentJob)
	d.Set("concurrency", job.Concurrency)
	d.Set("executor", job.Executor)
	d.Set("executor_config", job.ExecutorConfig)
	d.Set("metadata", job.Metadata)

	processors := make([]any, 0)
	for i, p := range job.Processors {
		processor := p.Config
		processor["type"] = i
		processors = append(processors, processor)
	}
	d.Set("processors", processors)

	return diags
}

func resourceDkronJobUpdate(ctx context.Context, d *schema.ResourceData, m any) diag.Diagnostics {
	old, new := d.GetChange("name")
	if old != new {
		resourceDkronJobDelete(ctx, d, m)
	}
	return resourceDkronJobCreate(ctx, d, m)
}

func resourceDkronJobDelete(ctx context.Context, d *schema.ResourceData, m any) diag.Diagnostics {
	var diags diag.Diagnostics

	config := m.(DkronConfig)
	client, err := dkron.NewClient(config.host)
	if err != nil {
		return diag.FromErr(err)
	}

	if _, err := client.DeleteJob(ctx, d.Id()); err != nil {
		return diag.FromErr(err)
	}

	return diags
}

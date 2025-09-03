package dkron

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	dkron "github.com/distribworks/dkron/v4/client"
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

type Processor = map[string]string

type Job struct {
	Name           string               `json:"name,omitempty"`
	Timezone       string               `json:"timezone,omitempty"`
	Schedule       string               `json:"schedule,omitempty"`
	Owner          string               `json:"owner,omitempty"`
	OwnerEmail     string               `json:"owner_email,omitempty"`
	SuccessCount   int32                `json:"success_count,omitempty"`
	ErrorCount     int32                `json:"error_count,omitempty"`
	Disabled       bool                 `json:"disabled,omitempty"`
	Tags           map[string]string    `json:"tags,omitempty"`
	Retries        int                  `json:"retries,omitempty"`
	DependentJobs  []string             `json:"dependent_jobs,omitempty"`
	ParentJob      string               `json:"parent_job,omitempty"`
	Concurrency    string               `json:"concurrency,omitempty"`
	Executor       string               `json:"executor,omitempty"`
	ExecutorConfig map[string]string    `json:"executor_config,omitempty"`
	Metadata       map[string]string    `json:"metadata,omitempty"`
	Displayname    string               `json:"displayname,omitempty"`
	Processors     map[string]Processor `json:"processors,omitempty"`
	Ephemeral      bool                 `json:"ephemeral,omitempty"`
}

func convertMap(m map[string]any) map[string]string {
	res := make(map[string]string, len(m))
	for k, v := range m {
		res[k] = v.(string)
	}
	return res
}

func resourceDkronJobCreate(ctx context.Context, d *schema.ResourceData, m any) diag.Diagnostics {
	var diags diag.Diagnostics

	config := m.(DkronConfig)
	client, err := dkron.NewClientWithResponses(config.host)
	if err != nil {
		return diag.FromErr(err)
	}

	body := Job{
		Ephemeral: false,
	}

	body.Name = d.Get("name").(string)
	body.Timezone = d.Get("timezone").(string)
	body.Displayname = d.Get("displayname").(string)
	body.Schedule = d.Get("schedule").(string)
	body.Owner = d.Get("owner").(string)
	body.OwnerEmail = d.Get("owner_email").(string)
	body.Disabled = d.Get("disabled").(bool)
	body.Tags = convertMap(d.Get("tags").(map[string]any))
	body.Retries = d.Get("retries").(int)
	body.ParentJob = d.Get("parent_job").(string)
	body.Concurrency = d.Get("concurrency").(string)
	body.Executor = d.Get("executor").(string)
	body.ExecutorConfig = convertMap(d.Get("executor_config").(map[string]any))
	body.Metadata = convertMap(d.Get("metadata").(map[string]any))
	body.Processors = make(map[string]Processor)

	processors := d.Get("processors").([]any)
	for _, p := range processors {
		processor := convertMap(p.(map[string]any))

		ty := processor["type"]
		delete(processor, "type")

		if processor["forward"] == "" {
			delete(processor, "forward")
		}
		if processor["log_dir"] == "" {
			delete(processor, "log_dir")
		}

		body.Processors[ty] = processor
	}

	buff, err := json.Marshal(body)
	if err != nil {
		return diag.FromErr(err)
	}

	response, err := client.CreateOrUpdateJobWithBody(
		ctx,
		nil,
		"application/json",
		bytes.NewBuffer(buff),
	)
	if err != nil {
		return diag.FromErr(err)
	}

	if response.StatusCode != 201 {
		body, _ := io.ReadAll(response.Body)
		return diag.FromErr(errors.New(string(body)))
	}

	d.SetId(body.Name)

	return diags
}

func resourceDkronJobRead(ctx context.Context, d *schema.ResourceData, m any) diag.Diagnostics {
	var diags diag.Diagnostics

	config := m.(DkronConfig)
	client, err := dkron.NewClient(config.host)
	if err != nil {
		return diag.FromErr(err)
	}

	response, err := client.ShowJobByName(ctx, d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	body, _ := io.ReadAll(response.Body)

	if response.StatusCode != 200 {
		return diag.FromErr(errors.New(string(body)))
	}

	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		return diag.FromErr(err)
	}

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
		p["type"] = i
		processors = append(processors, p)
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

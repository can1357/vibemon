{{/*
Expand the name of the chart.
*/}}
{{- define "vmon.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "vmon.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "vmon.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "vmon.labels" -}}
helm.sh/chart: {{ include "vmon.chart" . }}
{{ include "vmon.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "vmon.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vmon.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* API credential Secret, generated unless an external Secret is configured. */}}
{{- define "vmon.apiSecret" -}}
{{- default (printf "%s-api-secrets" (include "vmon.fullname" .)) .Values.existingSecret }}
{{- end }}

{{/* PostgreSQL credential Secret, generated unless an external Secret is configured. */}}
{{- define "vmon.postgresSecret" -}}
{{- default (printf "%s-postgres-secrets" (include "vmon.fullname" .)) .Values.existingSecret }}
{{- end }}

{{/* S3 credential Secret, generated unless an external Secret is configured. */}}
{{- define "vmon.s3Secret" -}}
{{- default (printf "%s-s3-secrets" (include "vmon.fullname" .)) .Values.existingSecret }}
{{- end }}

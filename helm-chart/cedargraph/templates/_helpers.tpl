{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "cedargraph.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "cedargraph.fullname" -}}
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
{{- define "cedargraph.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "cedargraph.labels" -}}
helm.sh/chart: {{ include "cedargraph.chart" . }}
{{ include "cedargraph.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "cedargraph.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cedargraph.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
MetaD 地址列表
*/}}
{{- define "cedargraph.metad.endpoints" -}}
{{- $fullname := include "cedargraph.fullname" . }}
{{- $port := .Values.metad.service.grpcPort }}
{{- range $i := until (int .Values.metad.replicas) }}
{{- if $i }},{{ end }}{{$fullname}}-metad-{{$i}}.{{$fullname}}-metad:{{$port}}
{{- end }}
{{- end }}

{{/*
MetaD Raft peers for cedar-metad --peer.
*/}}
{{- define "cedargraph.metad.raftPeerArgs" -}}
{{- $fullname := include "cedargraph.fullname" . }}
{{- $port := .Values.metad.service.port }}
{{- range $i := until (int .Values.metad.replicas) }}
{{- $nodeID := add $i 1 }}
--peer "{{ $nodeID }}:{{ $fullname }}-metad-{{ $i }}.{{ $fullname }}-metad:{{ $port }}"{{ if lt (add $i 1) (int $.Values.metad.replicas) }} \
{{ end }}
{{- end }}
{{- end }}

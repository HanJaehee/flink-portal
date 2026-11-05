{{- define "flink-portal.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "flink-portal.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains (include "flink-portal.name" .) .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "flink-portal.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- define "flink-portal.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flink-portal.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "flink-portal.labels" -}}
{{ include "flink-portal.selectorLabels" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
{{- define "flink-portal.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "flink-portal.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
{{- define "flink-portal.readerName" -}}
{{- $identity := printf "%s/%s" .Release.Namespace (include "flink-portal.fullname" .) -}}
{{- printf "%s-%s-reader" (include "flink-portal.fullname" . | trunc 40 | trimSuffix "-") (sha256sum $identity | trunc 8) -}}
{{- end -}}

{{- define "swytch-operator.name" -}}
{{- printf "%s-swytch-operator" .Release.Name | trunc 48 | trimSuffix "-" -}}
{{- end -}}

{{- define "swytch-operator.labels" -}}
app.kubernetes.io/name: swytch-operator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

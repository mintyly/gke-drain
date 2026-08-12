package cloud

import "time"

type enabledJSON struct {
	Enabled bool `json:"enabled"`
}

type clusterJSON struct {
	ID         string      `json:"id"`
	Name       string      `json:"name"`
	Status     string      `json:"status"`
	Endpoint   string      `json:"endpoint"`
	Locations  []string    `json:"locations"`
	Autopilot  enabledJSON `json:"autopilot"`
	MasterAuth struct {
		ClusterCACertificate string `json:"clusterCaCertificate"`
	} `json:"masterAuth"`
}

type poolJSON struct {
	Name              string      `json:"name"`
	Status            string      `json:"status"`
	Locations         []string    `json:"locations"`
	InstanceGroupURLs []string    `json:"instanceGroupUrls"`
	AutopilotConfig   enabledJSON `json:"autopilotConfig"`
	Autoscaling       struct {
		Enabled         bool `json:"enabled"`
		Autoprovisioned bool `json:"autoprovisioned"`
	} `json:"autoscaling"`
}

type operationJSON struct {
	Name          string `json:"name"`
	Status        string `json:"status"`
	StatusMessage string `json:"statusMessage"`
	TargetLink    string `json:"targetLink"`
	Zone          string `json:"zone"`
	Location      string `json:"location"`
	SelfLink      string `json:"selfLink"`
	Error         *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type ownerJSON struct {
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Controller bool   `json:"controller"`
}

type metadataJSON struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	UID               string            `json:"uid"`
	ResourceVersion   string            `json:"resourceVersion"`
	CreationTimestamp time.Time         `json:"creationTimestamp"`
	DeletionTimestamp *time.Time        `json:"deletionTimestamp"`
	Generation        int64             `json:"generation"`
	Labels            map[string]string `json:"labels"`
	Annotations       map[string]string `json:"annotations"`
	OwnerReferences   []ownerJSON       `json:"ownerReferences"`
}

type nodeJSON struct {
	Metadata metadataJSON `json:"metadata"`
	Spec     struct {
		ProviderID    string `json:"providerID"`
		Unschedulable bool   `json:"unschedulable"`
	} `json:"spec"`
	Status struct {
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
	} `json:"status"`
}

type podJSON struct {
	Metadata metadataJSON `json:"metadata"`
	Spec     struct {
		NodeName string `json:"nodeName"`
		Volumes  []struct {
			EmptyDir              *struct{} `json:"emptyDir"`
			HostPath              *struct{} `json:"hostPath"`
			PersistentVolumeClaim *struct{} `json:"persistentVolumeClaim"`
			Ephemeral             *struct{} `json:"ephemeral"`
		} `json:"volumes"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

type workloadJSON struct {
	Metadata metadataJSON `json:"metadata"`
	Spec     struct {
		Replicas *int `json:"replicas"`
	} `json:"spec"`
	Status struct {
		ReadyReplicas      int   `json:"readyReplicas"`
		ObservedGeneration int64 `json:"observedGeneration"`
	} `json:"status"`
}

#!/bin/sh

find vendor/github.com/operator-framework/operator-sdk -name '*.go' -exec sed -i "s/cached.NewMemCacheClient/memory.NewMemCacheClient/g" {} \;
find vendor -name '*.go' -exec sed -i 's/resourceClient.Create(unstructObj)/resourceClient.Create(unstructObj, metav1.CreateOptions\{\})/g' {} \;
find vendor -name '*.go' -exec sed -i 's/resourceClient.Update(unstructObj)/resourceClient.Update(unstructObj, metav1.UpdateOptions\{\})/g' {} \;
find vendor -name '*.go' -exec sed -i 's/resourceClient.Patch(name, pt, patch)/resourceClient.Patch(name, pt, patch, metav1.PatchOptions\{\})/g' {} \;
find vendor/github.com/openshift -name '*.go' -exec sed -i 's/DirectCodecFactory/WithoutConversionCodecFactory/g' {} \;
find vendor/github.com/libopenstorage/autopilot-api -name '*.go' -exec sed -i 's/DirectCodecFactory/WithoutConversionCodecFactory/g' {} \;
find vendor/github.com/openshift/client-go -name '*.go' -exec sed -i 's/Invokes(testing.NewPatchSubresourceAction(deploymentconfigsResource, c.ns, name, data/Invokes(testing.NewPatchSubresourceAction(deploymentconfigsResource, c.ns, name, pt, data/g' {} \;
find vendor/github.com/openshift/client-go/security/clientset -name '*.go' -exec sed -i 's/name, data/name, pt, data/g' {} \;


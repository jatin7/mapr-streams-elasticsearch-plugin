/**
 * 
 */
package org.elasticsearch.index.plugin;

import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.MapRStreamsESModule;
import org.elasticsearch.index.MapRStreamsESService;
import org.elasticsearch.plugins.Plugin;

/**
 * @author ntirupattur
 *
 */
public class MapRStreamsESPlugin extends Plugin {
	
	@Override
    public Collection<Module> createGuiceModules() {
        Collection<Module> extra = new ArrayList<>();
        extra.add(new MapRStreamsESModule());
        return extra;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        Collection<Class<? extends LifecycleComponent>> extra = new ArrayList<>();
        extra.add(MapRStreamsESService.class);
        return extra;
    }
	
}

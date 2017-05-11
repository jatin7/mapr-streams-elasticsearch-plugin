/**
 * 
 */
package org.elasticsearch.index;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.index.MapRStreamsESService;

/**
 * @author ntirupattur
 *
 */
public class MapRStreamsESModule extends AbstractModule {
	
	@Override
	protected void configure() {
		bind(MapRStreamsESService.class).asEagerSingleton();
	}
}

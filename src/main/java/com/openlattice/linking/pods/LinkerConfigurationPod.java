/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.linking.pods;

import com.amazonaws.services.s3.AmazonS3;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants.Profiles;
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.ResourceConfigurationLoader;
import com.openlattice.linking.LinkingConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.inject.Inject;
import java.io.IOException;

/**
 *
 */
@Configuration
public class LinkerConfigurationPod {
    private static final Logger               logger = LoggerFactory.getLogger( LinkerConfigurationPod.class );

    @Inject
    private              ConfigurationService configurationService;

    @Autowired( required = false )
    private              AmazonS3             s3;

    @Autowired( required = false )
    private AmazonLaunchConfiguration awsLaunchConfig;

    @Bean( name = "linkingConfiguration" )
    @Profile( Profiles.LOCAL_CONFIGURATION_PROFILE )
    public LinkingConfiguration getLocalLinkingConfiguration() throws IOException {
        LinkingConfiguration config = configurationService.getConfiguration( LinkingConfiguration.class );
        logger.info( "Using local linking configuration: {}", config );
        return config;
    }

    @Bean( name = "likningConfiguration" )
    @Profile( { Profiles.AWS_CONFIGURATION_PROFILE, Profiles.AWS_TESTING_PROFILE } )
    public LinkingConfiguration getAwsLinkingConfiguration() throws IOException {
        LinkingConfiguration config = ResourceConfigurationLoader.loadConfigurationFromS3( s3,
                awsLaunchConfig.getBucket(),
                awsLaunchConfig.getFolder(),
                LinkingConfiguration.class );

        logger.info( "Using aws linking configuration: {}", config );
        return config;
    }
}

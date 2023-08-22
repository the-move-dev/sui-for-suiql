// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { Outlet } from 'react-router-dom';
import { OnboardingFormProvider } from './ImportAccountsPage';
import { Toaster } from '../../shared/toaster';
import PageLayout from '_pages/layout';

export function AccountsPage() {
	return (
		<OnboardingFormProvider>
			<PageLayout>
				<Outlet />
				<Toaster bottomNavEnabled={false} />
			</PageLayout>
		</OnboardingFormProvider>
	);
}

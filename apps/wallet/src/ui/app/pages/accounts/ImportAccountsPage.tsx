// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { type ReactNode, createContext, useState, useContext, type Dispatch } from 'react';
import { Outlet } from 'react-router-dom';

// todo: fix the any's (any maybe re-evaluate this altogether when we have more time)
type OnboardingFormContextType = [any, Dispatch<any>];

const OnboardingFormContext = createContext<OnboardingFormContextType | null>(null);

export const OnboardingFormProvider = ({ children }: { children: ReactNode }) => {
	const value = useState<any>(null);
	return <OnboardingFormContext.Provider value={value}>{children}</OnboardingFormContext.Provider>;
};

export const useOnboardingFormContext = () => {
	const context = useContext(OnboardingFormContext);
	if (!context) {
		throw new Error('useOnboardingFormContext must be used within the OnboardingFormProvider');
	}
	return context;
};

export function ImportAccountsPage() {
	return <Outlet />;
}

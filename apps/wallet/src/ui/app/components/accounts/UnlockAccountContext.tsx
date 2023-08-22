// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import React, { createContext, useContext, useState, type ReactNode, useCallback } from 'react';

import { toast } from 'react-hot-toast';
import { UnlockAccountModal } from './UnlockAccountModal';
import { useBackgroundClient } from '../../hooks/useBackgroundClient';

interface UnlockAccountContextType {
	isUnlockModalOpen: boolean;
	accountIdToUnlock: string | null;
	unlockAccount: (accountId: string) => void;
	lockAccount: (accountId: string) => void;
	hideUnlockModal: () => void;
}

const UnlockAccountContext = createContext<UnlockAccountContextType | undefined>(undefined);

export const UnlockAccountProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
	const [isUnlockModalOpen, setIsUnlockModalOpen] = useState(false);
	const [accountIdToUnlock, setAccountIdToUnlock] = useState<string | null>(null);
	const backgroundClient = useBackgroundClient();
	const hideUnlockModal = useCallback(() => {
		setIsUnlockModalOpen(false);
		setAccountIdToUnlock(null);
	}, []);

	const unlockAccount = useCallback((accountId: string) => {
		setIsUnlockModalOpen(true);
		setAccountIdToUnlock(accountId);
	}, []);

	const lockAccount = useCallback(
		async (accountId: string) => {
			try {
				await backgroundClient.lockAccountSourceOrAccount({ id: accountId });
				toast.success('Account locked');
			} catch (e) {
				toast.error((e as Error).message || 'Failed to lock account');
			}
		},
		[backgroundClient],
	);

	return (
		<UnlockAccountContext.Provider
			value={{ isUnlockModalOpen, accountIdToUnlock, unlockAccount, hideUnlockModal, lockAccount }}
		>
			{children}
			<UnlockAccountModal
				onClose={hideUnlockModal}
				onSuccess={hideUnlockModal}
				accountId={accountIdToUnlock!}
				open={isUnlockModalOpen}
			/>
		</UnlockAccountContext.Provider>
	);
};

export const useUnlockAccount = (): UnlockAccountContextType => {
	const context = useContext(UnlockAccountContext);
	if (!context) {
		throw new Error('useUnlockAccount must be used within an UnlockAccountProvider');
	}
	return context;
};

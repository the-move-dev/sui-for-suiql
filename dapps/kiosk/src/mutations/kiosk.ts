// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { useMutation } from '@tanstack/react-query';

import { useOwnedKiosk } from '../hooks/kiosk';
import { OwnedObjectType } from '../components/Inventory/OwnedObjects';
import { TransactionBlock } from '@mysten/sui.js/transactions';
import { Kiosk, KioskClient, Network, createKioskAndShare } from '@mysten/kiosk';
import { useTransactionExecution } from '../hooks/useTransactionExecution';
import { useWalletKit } from '@mysten/wallet-kit';
// import { useRpc } from '../context/RpcClientContext';
import { toast } from 'react-hot-toast';
import { findActiveCap } from '../utils/utils';
import { SuiClient, getFullnodeUrl } from '@mysten/sui.js/client';

const kioskClient = new KioskClient({
	client: new SuiClient({
		url: getFullnodeUrl('testnet'),
	}),
	network: Network.TESTNET,
});

type MutationParams = {
	onSuccess?: () => void;
	onError?: (e: Error) => void;
};

const defaultOnError = (e: Error) => {
	if (typeof e === 'string') toast.error(e);
	else toast.error(e?.message);
};

/**
 * Create a new kiosk.
 */
export function useCreateKioskMutation({ onSuccess, onError }: MutationParams) {
	const { currentAccount } = useWalletKit();
	const { signAndExecute } = useTransactionExecution();

	return useMutation({
		mutationFn: () => {
			if (!currentAccount?.address) throw new Error('You need to connect your wallet!');
			const tx = new TransactionBlock();
			const kiosk_cap = createKioskAndShare(tx);
			tx.transferObjects([kiosk_cap], tx.pure(currentAccount.address, 'address'));
			return signAndExecute({ tx });
		},
		onSuccess,
		onError: onError || defaultOnError,
	});
}

/**
 * Place & List or List for sale in kiosk.
 */
export function usePlaceAndListMutation({ onSuccess, onError }: MutationParams) {
	const { currentAccount } = useWalletKit();
	const { data: ownedKiosk } = useOwnedKiosk(currentAccount?.address);
	const { signAndExecute } = useTransactionExecution();

	return useMutation({
		mutationFn: async ({
			item,
			price,
			shouldPlace,
			kioskId,
		}: {
			item: OwnedObjectType;
			price: string;
			shouldPlace?: boolean;
			kioskId: string;
		}) => {
			// find active kiosk cap.
			const cap = findActiveCap(ownedKiosk?.caps, kioskId);

			if (!cap || !currentAccount?.address) throw new Error('Missing account, kiosk or kiosk cap');

			const tx = new TransactionBlock();

			await kioskClient.ownedKioskTx(tx, cap, async (tx, kioskId, capObject) => {
				if (shouldPlace)
					kioskClient.placeAndList(tx, item.type, item.objectId, price, kioskId, capObject);
				else kioskClient.list(tx, item.type, item.objectId, price, kioskId, capObject);
			});

			return signAndExecute({ tx });
		},
		onSuccess,
		onError: onError || defaultOnError,
	});
}

/**
 * Mutation to place an item in the kiosk.
 */
export function usePlaceMutation({ onSuccess, onError }: MutationParams) {
	const { currentAccount } = useWalletKit();
	const { data: ownedKiosk } = useOwnedKiosk(currentAccount?.address);
	const { signAndExecute } = useTransactionExecution();

	return useMutation({
		mutationFn: async ({ item, kioskId }: { item: OwnedObjectType; kioskId: string }) => {
			// find active kiosk cap.
			const cap = findActiveCap(ownedKiosk?.caps, kioskId);

			if (!cap || !currentAccount?.address) throw new Error('Missing account, kiosk or kiosk cap');

			const tx = new TransactionBlock();

			await kioskClient.ownedKioskTx(tx, cap, async (tx, kioskId, capObject) => {
				kioskClient.place(tx, item.type, item.objectId, kioskId, capObject);
			});

			return signAndExecute({ tx });
		},
		onSuccess,
		onError: onError || defaultOnError,
	});
}

/**
 * Withdraw profits from kiosk
 */
export function useWithdrawMutation({ onError, onSuccess }: MutationParams) {
	const { currentAccount } = useWalletKit();
	const { data: ownedKiosk } = useOwnedKiosk(currentAccount?.address);
	const { signAndExecute } = useTransactionExecution();

	return useMutation({
		mutationFn: async (kiosk: Kiosk) => {
			// find active kiosk cap.
			const cap = findActiveCap(ownedKiosk?.caps, kiosk.id);

			if (!cap || !currentAccount?.address) throw new Error('Missing account, kiosk or kiosk cap');

			const tx = new TransactionBlock();

			await kioskClient.ownedKioskTx(tx, cap, async (tx, kioskId, capObject) => {
				const coin = kioskClient.withdraw(tx, kioskId, capObject, kiosk.profits);

				tx.transferObjects([coin], tx.pure(currentAccount.address, 'address'));
			});

			return signAndExecute({ tx });
		},
		onSuccess,
		onError: onError || defaultOnError,
	});
}

/**
 * Mutation to take an item from the kiosk.
 */
export function useTakeMutation({ onSuccess, onError }: MutationParams) {
	const { currentAccount } = useWalletKit();
	const { data: ownedKiosk } = useOwnedKiosk(currentAccount?.address);
	const { signAndExecute } = useTransactionExecution();

	return useMutation({
		mutationFn: async ({ item, kioskId }: { item: OwnedObjectType; kioskId: string }) => {
			// find active kiosk cap.
			const cap = findActiveCap(ownedKiosk?.caps, kioskId);

			if (!cap || !currentAccount?.address) throw new Error('Missing account, kiosk or kiosk cap');

			if (!item?.objectId) throw new Error('Missing item.');

			const tx = new TransactionBlock();

			await kioskClient.ownedKioskTx(tx, cap, async (tx, kioskId, capObject) => {
				const obj = kioskClient.take(tx, item.type, item.objectId, kioskId, capObject);
				tx.transferObjects([obj], tx.pure(currentAccount?.address));
			});

			return signAndExecute({ tx });
		},
		onSuccess,
		onError: onError || defaultOnError,
	});
}

/**
 * Mutation to delist an item.
 */
export function useDelistMutation({ onSuccess, onError }: MutationParams) {
	const { currentAccount } = useWalletKit();
	const { data: ownedKiosk } = useOwnedKiosk(currentAccount?.address);
	const { signAndExecute } = useTransactionExecution();

	return useMutation({
		mutationFn: async ({ item, kioskId }: { item: OwnedObjectType; kioskId: string }) => {
			// find active kiosk cap.
			const cap = findActiveCap(ownedKiosk?.caps, kioskId);

			if (!cap || !currentAccount?.address) throw new Error('Missing account, kiosk or kiosk cap');

			if (!item?.objectId) throw new Error('Missing item.');

			const tx = new TransactionBlock();

			await kioskClient.ownedKioskTx(tx, cap, async (tx, kioskId, capObject) => {
				kioskClient.delist(tx, item.type, item.objectId, kioskId, capObject);
			});

			return signAndExecute({ tx });
		},
		onSuccess,
		onError: onError || defaultOnError,
	});
}

/**
 * Mutation to delist an item.
 */
export function usePurchaseItemMutation({ onSuccess, onError }: MutationParams) {
	const { currentAccount } = useWalletKit();
	const { data: ownedKiosk } = useOwnedKiosk(currentAccount?.address);
	const { signAndExecute } = useTransactionExecution();

	return useMutation({
		mutationFn: async ({ item, kioskId }: { item: OwnedObjectType; kioskId: string }) => {
			if (
				!item ||
				!item.listing?.price ||
				!kioskId ||
				!currentAccount?.address ||
				!ownedKiosk?.kioskId ||
				!ownedKiosk.kioskCap
			)
				throw new Error('Missing parameters');

			const cap = findActiveCap(ownedKiosk?.caps, ownedKiosk.kioskId);
			if (!cap || !currentAccount?.address) throw new Error('Missing account, kiosk or kiosk cap');

			const tx = new TransactionBlock();

			await kioskClient.ownedKioskTx(tx, cap, async (tx, ownedKioskId, capObject) => {
				await kioskClient.purchaseAndResolve(tx, item, kioskId, ownedKioskId, capObject);
			});

			return await signAndExecute({ tx });
		},
		onSuccess,
		onError: onError || defaultOnError,
	});
}
